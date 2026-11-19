package reward

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestWebhookResponseClassification(t *testing.T) {
	for _, test := range []struct {
		code int
		want string
	}{
		{http.StatusNoContent, "delivered"},
		{http.StatusTooManyRequests, "pending"},
		{http.StatusServiceUnavailable, "pending"},
		{http.StatusForbidden, "failed"},
	} {
		t.Run(http.StatusText(test.code), func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(test.code)
			}))
			defer server.Close()
			event := Event{Publisher: "demo", EventID: "id", UserID: "user", Amount: 10}
			got, _ := deliver(context.Background(), server.Client(), event, Partner{URL: server.URL, Secret: "secret"})
			if got != test.want {
				t.Fatalf("got %q, want %q", got, test.want)
			}
		})
	}
}

func TestBackoffCaps(t *testing.T) {
	const minimum, maximum = time.Second, 3 * time.Second
	for _, test := range []struct {
		attempts int64
		want     time.Duration
	}{{1, time.Second}, {2, 2 * time.Second}, {3, maximum}, {1000, maximum}} {
		if got := backoff(minimum, maximum, test.attempts); got != test.want {
			t.Errorf("attempt %d: got %v, want %v", test.attempts, got, test.want)
		}
	}
}
