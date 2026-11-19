package reward

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net/http"
	"sync"
	"time"
)

type Partner struct {
	URL    string
	Secret string
}

type Dispatcher struct {
	Store      Store
	Partners   map[string]Partner
	Workers    int
	Timeout    time.Duration
	Lease      time.Duration
	Poll       time.Duration
	MinBackoff time.Duration
	MaxBackoff time.Duration
}

const dbTimeout = 2 * time.Second

func (d Dispatcher) Run(ctx context.Context) error {
	if d.Workers < 1 || d.Timeout <= 0 || d.Lease-d.Timeout <= 2*dbTimeout ||
		d.Poll <= 0 || d.MinBackoff <= 0 || d.MaxBackoff < d.MinBackoff {
		return errors.New("invalid dispatcher configuration")
	}
	client := &http.Client{
		CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse },
	}
	defer client.CloseIdleConnections()

	var workers sync.WaitGroup
	for i := 0; i < d.Workers; i++ {
		workers.Add(1)
		go func() {
			defer workers.Done()
			d.work(ctx, client)
		}()
	}
	workers.Wait()
	return nil
}

func (d Dispatcher) work(ctx context.Context, client *http.Client) {
	for ctx.Err() == nil {
		claimCtx, cancel := context.WithTimeout(ctx, dbTimeout)
		job, err := d.Store.Claim(claimCtx, d.Lease)
		cancel()
		if err == nil {
			// Stop taking claims on shutdown. Give an already claimed job its bounded attempt and write.
			d.process(context.WithoutCancel(ctx), client, job)
			continue
		}
		if ctx.Err() == nil && !errors.Is(err, sql.ErrNoRows) {
			log.Printf("claim reward: %v", err)
		}
		timer := time.NewTimer(d.Poll)
		select {
		case <-ctx.Done():
			timer.Stop()
		case <-timer.C:
		}
	}
}

func (d Dispatcher) process(ctx context.Context, client *http.Client, job Record) {
	attemptCtx, cancel := context.WithTimeout(ctx, d.Timeout)
	status, reason := deliver(attemptCtx, client, job.Event, d.Partners[job.Publisher])
	cancel()
	var delay time.Duration
	if status == "pending" {
		delay = backoff(d.MinBackoff, d.MaxBackoff, job.Attempts)
	}
	saveCtx, cancel := context.WithTimeout(ctx, dbTimeout)
	defer cancel()
	saved, err := d.Store.Finish(saveCtx, job, status, reason, delay)
	if err != nil {
		log.Printf("save reward attempt %s/%s: %v", job.Publisher, job.EventID, err)
	} else if !saved {
		log.Printf("reward lease expired for %s/%s attempt %d", job.Publisher, job.EventID, job.Attempts)
	}
}

func backoff(minimum, maximum time.Duration, attempts int64) time.Duration {
	delay := minimum
	for i := int64(1); i < attempts && delay < maximum; i++ {
		if delay > maximum/2 {
			return maximum
		}
		delay *= 2
	}
	return delay
}

func deliver(ctx context.Context, client *http.Client, event Event, partner Partner) (status, reason string) {
	if partner.URL == "" || partner.Secret == "" {
		return "failed", "publisher is no longer configured"
	}
	body, err := json.Marshal(event)
	if err != nil {
		return "failed", err.Error()
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, partner.URL, bytes.NewReader(body))
	if err != nil {
		return "failed", err.Error()
	}
	req.GetBody = nil // An application retry should be visible as another persisted attempt.
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Idempotency-Key", idempotencyKey(event))
	req.Header.Set("X-Reward-Signature", signature(partner.Secret, body))

	resp, err := client.Do(req)
	if err != nil {
		if errors.Is(ctx.Err(), context.DeadlineExceeded) {
			return "pending", "webhook deadline exceeded"
		}
		return "pending", "webhook transport error"
	}
	defer resp.Body.Close()
	switch {
	case resp.StatusCode >= 200 && resp.StatusCode < 300:
		return "delivered", ""
	case resp.StatusCode == http.StatusTooManyRequests || (resp.StatusCode >= 500 && resp.StatusCode < 600):
		return "pending", fmt.Sprintf("publisher returned HTTP %d", resp.StatusCode)
	default:
		return "failed", fmt.Sprintf("publisher returned HTTP %d", resp.StatusCode)
	}
}

func idempotencyKey(event Event) string {
	// API identifiers exclude colon, so the two components cannot collide.
	return event.Publisher + ":" + event.EventID
}

func signature(secret string, body []byte) string {
	mac := hmac.New(sha256.New, []byte(secret))
	mac.Write(body)
	return hex.EncodeToString(mac.Sum(nil))
}
