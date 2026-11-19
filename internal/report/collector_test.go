package report

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func TestSnapshotRerunAndSecondPageFailure(t *testing.T) {
	const day = "2026-01-15"
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	csv := httptest.NewServer(MockReport(CSVDollars, false))
	defer csv.Close()
	micros := httptest.NewServer(MockReport(JSONMicros, false))
	defer micros.Close()
	var fail atomic.Bool
	var failedRunFirstPages atomic.Int32
	cents := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if fail.Load() && r.URL.Query().Get("cursor") == "" {
			failedRunFirstPages.Add(1)
			// A changed first page would corrupt totals if it were published early.
			w.Write([]byte(`{"rows":[{"publisher":"alpha","impressions":900,"revenue":900}],"next_cursor":"second"}`))
			return
		}
		MockReport(JSONCents, fail.Load()).ServeHTTP(w, r)
	}))
	defer cents.Close()
	providers := MockProviders(csv.URL, cents.URL, micros.URL)
	dir := t.TempDir()
	want := map[string]Totals{
		"cents":  {Impressions: 100, USDMicros: 1_500_000},
		"csv":    {Impressions: 170, USDMicros: 1_325_001},
		"micros": {Impressions: 100, USDMicros: 150_000},
	}

	baseline := make(map[string][]byte)
	for run := 0; run < 2; run++ {
		results, err := Collect(ctx, day, dir, providers, 2)
		if err != nil || len(results) != 3 {
			t.Fatalf("run %d: results=%+v error=%v", run, results, err)
		}
		for _, result := range results {
			if result.Total != want[result.Provider] {
				t.Fatalf("%s totals=%+v, want %+v", result.Provider, result.Total, want[result.Provider])
			}
			body := readSnapshot(t, dir, result.Provider, day)
			if old := baseline[result.Provider]; old != nil && !bytes.Equal(body, old) {
				t.Fatalf("%s changed on rerun", result.Provider)
			}
			var snapshot Snapshot
			err := json.Unmarshal(body, &snapshot)
			if err != nil || snapshot.Date != day || snapshot.Provider != result.Provider || snapshot.Total != result.Total {
				t.Fatalf("%s unexpected snapshot: %+v, %v", result.Provider, snapshot, err)
			}
			if result.Provider == "csv" && snapshot.Publishers["alpha"] != (Totals{Impressions: 120, USDMicros: 1_125_001}) {
				t.Fatalf("unexpected CSV grouping: %+v", snapshot.Publishers)
			}
			baseline[result.Provider] = body
		}
	}

	fail.Store(true)
	results, err := Collect(ctx, day, dir, providers, 2)
	if err == nil || len(results) != 3 || failedRunFirstPages.Load() != 1 {
		t.Fatalf("partial failure: first pages=%d results=%+v error=%v", failedRunFirstPages.Load(), results, err)
	}
	for _, result := range results {
		if (result.Err != nil) != (result.Provider == "cents") {
			t.Fatalf("unexpected provider outcome: %+v", result)
		}
		if body := readSnapshot(t, dir, result.Provider, day); !bytes.Equal(body, baseline[result.Provider]) {
			t.Fatalf("%s previous snapshot changed", result.Provider)
		}
	}
}

func readSnapshot(t *testing.T, dir, provider, day string) []byte {
	t.Helper()
	body, err := os.ReadFile(filepath.Join(dir, provider+"-"+day+".json"))
	if err != nil {
		t.Fatal(err)
	}
	return body
}

func TestGlobalConcurrencyAndCancellation(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	started := make(chan struct{}, 3)
	finished := make(chan struct{}, 3)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.(http.Flusher).Flush() // Only the headers are sent. Body processing is still active.
		started <- struct{}{}
		<-r.Context().Done()
		finished <- struct{}{}
	}))
	defer server.Close()
	defer cancel()
	providers := MockProviders(server.URL, server.URL, server.URL)
	dir := t.TempDir()
	done := make(chan error, 1)
	go func() {
		_, err := Collect(ctx, "2026-01-15", dir, providers, 2)
		done <- err
	}()
	for i := 0; i < 2; i++ {
		select {
		case <-started:
		case <-ctx.Done():
			t.Fatal("two providers did not start concurrently")
		}
	}
	select {
	case <-started:
		t.Fatal("third provider started while both slots were in use")
	case <-time.After(40 * time.Millisecond):
	}
	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("collect returned %v, want context cancellation", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("collector did not exit after cancellation")
	}
	for i := 0; i < 2; i++ {
		select {
		case <-finished:
		case <-time.After(2 * time.Second):
			t.Fatal("provider request was not canceled")
		}
	}
}

func TestUSDMicros(t *testing.T) {
	for _, test := range []struct {
		value string
		want  int64
	}{
		{"12", 12_000_000}, {"0.000001", 1}, {"2.5", 2_500_000},
		{"9223372036854.775807", 9223372036854775807},
	} {
		got, err := usdMicros(test.value)
		if err != nil || got != test.want {
			t.Errorf("%q produced %d, %v, want %d", test.value, got, err, test.want)
		}
	}
	for _, value := range []string{"-1", "0.0000001", "1.", "1e2", "9223372036854.775808"} {
		if _, err := usdMicros(value); err == nil {
			t.Errorf("accepted invalid USD amount %q", value)
		}
	}
}

func BenchmarkParseCSV(b *testing.B) {
	const rows = 100_000
	data := "publisher,impressions,revenue_usd\n" + strings.Repeat("alpha,100,1.125001\nbeta,25,0.250000\n", rows/2)
	b.ReportAllocs()
	b.SetBytes(int64(len(data)))
	ctx := context.Background()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		snapshot := Snapshot{Publishers: make(map[string]Totals)}
		if err := ParseCSV(ctx, strings.NewReader(data), &snapshot); err != nil {
			b.Fatal(err)
		}
		if snapshot.Total != (Totals{Impressions: 6_250_000, USDMicros: 68_750_050_000}) {
			b.Fatalf("unexpected totals: %+v", snapshot.Total)
		}
	}
}
