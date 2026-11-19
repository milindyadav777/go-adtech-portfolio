# Go ad tech portfolio

One Go module with two runnable commands. Both demos use synthetic data and loopback HTTP mocks.

## Run the demos

Use Go 1.23 or later. Run these commands from the module directory.

### Reward callback dispatcher

```sh
docker compose up -d --wait
export DATABASE_URL='postgres://portfolio:portfolio@127.0.0.1:5432/portfolio?sslmode=disable'
go run ./cmd/reward-dispatcher -demo
```

The demo uses a temporary PostgreSQL schema and drops it on exit. Its database role needs permission to create schemas. Expected output:

```text
submit amount=25 HTTP 202
submit amount=25 HTTP 200
submit amount=26 HTTP 409
status=delivered attempts=2 webhook_calls=2 publisher_credits=1
```

To leave the API running, start a local receiver, set its URL and secret, and run the dispatcher without `-demo`. The command configures one synthetic publisher named `demo`.

```sh
export WEBHOOK_SECRET='local-demo-secret'
go run ./cmd/reward-dispatcher -webhook-url http://127.0.0.1:8081/webhook
```

In a second terminal:

```sh
curl -i http://127.0.0.1:8080/events \
  -H 'Content-Type: application/json' \
  -d '{"publisher":"demo","event_id":"reward-1","user_id":"user-42","amount":25}'

curl -i http://127.0.0.1:8080/events/demo/reward-1
```

| Request | Result |
| --- | --- |
| Submit a new publisher and event ID | `202` with its pending record |
| Submit the same event again | `200` with the current record |
| Submit a different user or amount with the same publisher and event ID | `409` |
| Check delivery status | `200` with status, attempt count, and any recent error or pending retry time |
| Check an unknown event | `404` |

The dispatcher sends `X-Reward-Signature` as the hex-encoded HMAC-SHA256 of the exact JSON request body and reuses `Idempotency-Key: <publisher>:<event_id>` on every attempt. The demo receiver verifies both headers and applies one credit per key under a mutex.

Delivery is **at least once**. A publisher can commit a credit before its HTTP reply reaches the worker, or a dispatcher can crash before saving a successful response. The worker sends again after a timeout or lease expiry. A real publisher needs to persist the idempotency key and the balance change in the same database transaction. The demo receiver keeps its credits in memory.

A worker claims one row with `FOR UPDATE SKIP LOCKED` and increments its attempt number while setting a PostgreSQL lease. The outbound HTTP call happens after that statement commits. The final write requires the same attempt number and an active lease. If a process dies, another worker can claim the row after expiry. A claim counts as an attempt even if the worker crashes before sending. Three workers bound the number of active webhook attempts. On SIGINT or SIGTERM the API stops accepting connections, and workers stop claiming jobs and finish any already claimed work within the HTTP and database deadlines.

Timeouts, transport failures, HTTP 429 and 5xx retry with exponential backoff capped at 10 seconds in the command. Other non-2xx responses are terminal. Transient failures keep retrying at the cap. The status endpoint reports the last recorded outcome, so a pending event can be either leased or queued for another attempt.

### Publisher reporting collector

```sh
go run ./cmd/report-collector -date 2026-01-15 -out snapshots
./scripts/demo-reports.sh
```

The CLI starts three local mock servers by default. The script runs the CLI twice, compares the snapshots byte for byte, then injects a page-two failure and checks the exit code and saved snapshots. You can also run with `-fail-second-page` directly or supply all three local endpoint URLs via `-csv-url`, `-cents-url`, and `-micros-url`.

| Provider | Input | Impressions | Revenue (USD micros) |
| --- | --- | ---: | ---: |
| `cents` | Paginated JSON, USD cents | 100 | 1,500,000 |
| `csv` | CSV, decimal USD | 170 | 1,325,001 |
| `micros` | Paginated JSON, integer USD micros | 100 | 150,000 |

Each snapshot contains the provider, UTC report date, counts and USD micros by publisher, and provider totals. Decimal conversion is integer arithmetic. Inputs with fractional USD precision beyond six digits are rejected.

One goroutine collects each provider. A shared channel caps simultaneous requests, including time spent reading the response body. Each provider spaces its request starts and follows cursors sequentially. Waiting for slots or rate limits and reading HTTP bodies all honor cancellation. Requests get a five-second deadline. CSV is read one record at a time with `encoding/csv`, and the collector retains one aggregate per publisher.

The collector aggregates a provider's full report before writing a temporary file and renaming it over the previous provider/date snapshot. On local Linux filesystems the rename publishes a complete file atomically. If a provider fails midway, its previous file remains visible, other providers can publish, and the CLI exits with code 1. Concurrent successful runs for the same provider/date use last writer wins.

## Verification

```sh
go vet ./...
go test -race -count=1 ./...
TEST_DATABASE_URL="$DATABASE_URL" go test -race -count=1 -run '^TestIntegration' -v ./internal/reward
go test ./internal/report -run '^$' -bench '^BenchmarkParseCSV$' -benchmem -count=3
```

The CSV benchmark generates 100,000 rows across two publishers before timing. It measures reading, normalizing and aggregating one report per operation, with no HTTP or snapshot I/O in the timed section.

Measured on Linux amd64 with Go 1.24.4 and an Intel Xeon Platinum 8370C across three runs. Each 100,000-row report took 21.68–22.83 ms, used about 1.60 MB of cumulative allocations in 100,014 allocations, and processed 78.84–83.02 MB/s. The allocation total measures allocations over the full run. Live aggregation memory depends on the number of publishers.

## Interview walkthrough

| Decision | Start here | Verification |
| --- | --- | --- |
| Idempotent submission and safe recovery | [`Store.Submit`, `Store.Claim`, `Store.Finish`](internal/reward/store.go) | [`TestIntegrationReclaimExpiredLease`](internal/reward/integration_test.go) shows an expired attempt cannot update the record even after it is reclaimed. |
| Worker bounds, HTTP deadlines, and retry | [`Dispatcher.Run`, `Dispatcher.process`, `deliver`](internal/reward/dispatcher.go) and [`MockPublisher.ServeHTTP`](internal/reward/mock.go) | [`TestIntegrationTimeoutAfterCredit`](internal/reward/integration_test.go) exercises duplicate submission, the conflict response, two HTTP attempts, one credit, and the final status. [`TestIntegrationShutdownDrainsClaimedAttempt`](internal/reward/integration_test.go) checks that shutdown completes the claimed job and leaves the next job queued. |
| Concurrent fetches and atomic snapshots | [`Collect`, `fetchPage`, `publish`](internal/report/collector.go) | [`TestGlobalConcurrencyAndCancellation` and `TestSnapshotRerunAndSecondPageFailure`](internal/report/collector_test.go) exercise the HTTP bound, cancellation, reruns, and a mid-pagination failure. |
| Fixed-point currency and streaming CSV | [`ParseCSV` and `usdMicros`](internal/report/csv.go) | [`TestUSDMicros` and `BenchmarkParseCSV`](internal/report/collector_test.go) cover currency boundaries and measure large-report processing. |
