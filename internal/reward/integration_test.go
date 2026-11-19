package reward

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"
	"time"

	_ "github.com/lib/pq"
)

func integrationStore(t *testing.T, ctx context.Context) Store {
	t.Helper()
	dsn := os.Getenv("TEST_DATABASE_URL")
	if dsn == "" {
		t.Skip("set TEST_DATABASE_URL to run PostgreSQL integration tests")
	}
	db, cleanup, err := IsolatedDatabase(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := cleanup(); err != nil {
			t.Errorf("clean integration schema: %v", err)
		}
	})
	store := Store{DB: db}
	if err := store.Init(ctx); err != nil {
		t.Fatal(err)
	}
	return store
}

func TestIntegrationTimeoutAfterCredit(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	store := integrationStore(t, ctx)
	mock := &MockPublisher{Secret: "test-secret", LoseFirstReply: true}
	publisher := httptest.NewServer(mock)
	defer publisher.Close()

	partners := map[string]Partner{"demo": {URL: publisher.URL, Secret: mock.Secret}}
	api := API(store, partners)
	event := Event{Publisher: "demo", EventID: "reward-1", UserID: "user-9", Amount: 10}
	for _, expected := range []struct {
		amount int64
		status int
	}{{10, 202}, {10, 200}, {11, 409}} {
		event.Amount = expected.amount
		body, err := json.Marshal(event)
		if err != nil {
			t.Fatal(err)
		}
		response := httptest.NewRecorder()
		api.ServeHTTP(response, httptest.NewRequestWithContext(ctx, http.MethodPost, "/events", bytes.NewReader(body)))
		if response.Code != expected.status {
			t.Fatalf("POST returned %d, want %d: %s", response.Code, expected.status, response.Body)
		}
		if expected.status != 409 {
			var record Record
			if err := json.Unmarshal(response.Body.Bytes(), &record); err != nil || record.Event != event || record.Status != "pending" {
				t.Fatalf("POST response record=%+v error=%v", record, err)
			}
		}
	}

	dispatcher := Dispatcher{
		Store: store, Partners: partners, Workers: 2, Timeout: 500 * time.Millisecond,
		Lease: 6 * time.Second, Poll: 5 * time.Millisecond, MinBackoff: 120 * time.Millisecond, MaxBackoff: 500 * time.Millisecond,
	}
	workCtx, stop := context.WithCancel(ctx)
	done := make(chan error, 1)
	go func() { done <- dispatcher.Run(workCtx) }()
	defer func() {
		stop()
		if err := <-done; err != nil {
			t.Error(err)
		}
	}()

	ticker := time.NewTicker(5 * time.Millisecond)
	defer ticker.Stop()
retry:
	for {
		record, err := store.Get(ctx, event.Publisher, event.EventID)
		if err != nil {
			t.Fatal(err)
		}
		switch record.Status {
		case "failed":
			t.Fatalf("webhook permanently failed: %s", record.LastError)
		case "delivered":
			if record.Attempts != 2 || record.NextAttempt != nil || record.LastError != "" {
				t.Fatalf("unexpected final record: %+v", record)
			}
			break retry
		}
		select {
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		case <-ticker.C:
		}
	}

	calls, credits := mock.Counts()
	if calls != 2 || credits != 1 {
		t.Fatalf("publisher requests=%d credits=%d, want 2 requests and 1 credit", calls, credits)
	}
	response := httptest.NewRecorder()
	api.ServeHTTP(response, httptest.NewRequestWithContext(ctx, http.MethodGet, "/events/demo/reward-1", nil))
	var final Record
	err := json.Unmarshal(response.Body.Bytes(), &final)
	if response.Code != http.StatusOK || err != nil || final.Status != "delivered" || final.Amount != 10 {
		t.Fatalf("GET status=%d record=%+v error=%v", response.Code, final, err)
	}
}

func TestIntegrationReclaimExpiredLease(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	store := integrationStore(t, ctx)
	event := Event{Publisher: "demo", EventID: "reward-1", UserID: "user-9", Amount: 10}
	if _, created, err := store.Submit(ctx, event); err != nil || !created {
		t.Fatalf("submit created=%t error=%v", created, err)
	}

	first, err := store.Claim(ctx, time.Minute)
	if err != nil || first.Attempts != 1 {
		t.Fatalf("first claim=%+v error=%v", first, err)
	}
	if _, err := store.Claim(ctx, time.Minute); !errors.Is(err, sql.ErrNoRows) {
		t.Fatalf("claim before lease expiration: %v", err)
	}
	if _, err := store.DB.ExecContext(ctx, "UPDATE reward_events SET lease_until = CURRENT_TIMESTAMP - INTERVAL '1 second'"); err != nil {
		t.Fatal(err)
	}
	if saved, err := store.Finish(ctx, first, "delivered", "", 0); saved || err != nil {
		t.Fatalf("expired worker saved=%t error=%v", saved, err)
	}

	second, err := store.Claim(ctx, time.Minute)
	if err != nil || second.Attempts != 2 || second.Event != first.Event {
		t.Fatalf("recovered claim=%+v error=%v", second, err)
	}
	if saved, err := store.Finish(ctx, first, "failed", "stale result", 0); saved || err != nil {
		t.Fatalf("stale worker saved=%t error=%v", saved, err)
	}
	if saved, err := store.Finish(ctx, second, "delivered", "", 0); !saved || err != nil {
		t.Fatalf("current worker saved=%t error=%v", saved, err)
	}
}

func TestIntegrationShutdownDrainsClaimedAttempt(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
	defer cancel()
	store := integrationStore(t, ctx)
	first := Event{Publisher: "demo", EventID: "first", UserID: "user-9", Amount: 10}
	if _, _, err := store.Submit(ctx, first); err != nil {
		t.Fatal(err)
	}

	started := make(chan struct{}, 1)
	reply, sendReply := context.WithCancel(context.Background())
	publisher := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		started <- struct{}{}
		select {
		case <-reply.Done():
			w.WriteHeader(http.StatusNoContent)
		case <-r.Context().Done():
		}
	}))
	defer publisher.Close()

	dispatcher := Dispatcher{
		Store: store, Partners: map[string]Partner{"demo": {URL: publisher.URL, Secret: "test-secret"}},
		Workers: 1, Timeout: 2 * time.Second, Lease: 7 * time.Second, Poll: 5 * time.Millisecond,
		MinBackoff: 20 * time.Millisecond, MaxBackoff: time.Second,
	}
	workCtx, stop := context.WithCancel(ctx)
	done := make(chan struct{})
	var runErr error
	go func() {
		defer close(done)
		runErr = dispatcher.Run(workCtx)
	}()
	defer func() {
		stop()
		sendReply()
		<-done
	}()

	select {
	case <-started:
	case <-ctx.Done():
		t.Fatal("worker did not start the webhook")
	}
	second := Event{Publisher: "demo", EventID: "second", UserID: "user-9", Amount: 10}
	if _, _, err := store.Submit(ctx, second); err != nil {
		t.Fatal(err)
	}
	stop()
	sendReply()
	select {
	case <-done:
		if runErr != nil {
			t.Fatal(runErr)
		}
	case <-ctx.Done():
		t.Fatal("worker did not exit")
	}

	for _, want := range []struct {
		id       string
		status   string
		attempts int64
	}{{first.EventID, "delivered", 1}, {second.EventID, "pending", 0}} {
		got, err := store.Get(ctx, "demo", want.id)
		if err != nil || got.Status != want.status || got.Attempts != want.attempts {
			t.Fatalf("%s record=%+v error=%v, want status=%s attempts=%d", want.id, got, err, want.status, want.attempts)
		}
	}
}
