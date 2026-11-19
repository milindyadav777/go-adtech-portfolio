package main

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"time"

	"adtech-portfolio/internal/reward"
)

func runDemo(ctx context.Context, api http.Handler, publisher *reward.MockPublisher) error {
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	server := httptest.NewServer(api)
	defer server.Close()

	var unique [12]byte
	if _, err := rand.Read(unique[:]); err != nil {
		return err
	}
	event := reward.Event{Publisher: "demo", EventID: fmt.Sprintf("example-%x", unique), UserID: "user-42", Amount: 25}
	client := server.Client()

	for _, submission := range []struct {
		amount int64
		want   int
	}{{25, http.StatusAccepted}, {25, http.StatusOK}, {26, http.StatusConflict}} {
		event.Amount = submission.amount
		body, err := json.Marshal(event)
		if err != nil {
			return err
		}
		req, err := http.NewRequestWithContext(ctx, http.MethodPost, server.URL+"/events", bytes.NewReader(body))
		if err != nil {
			return err
		}
		req.Header.Set("Content-Type", "application/json")
		resp, err := client.Do(req)
		if err != nil {
			return err
		}
		resp.Body.Close()
		if resp.StatusCode != submission.want {
			return fmt.Errorf("submit HTTP %d, want %d", resp.StatusCode, submission.want)
		}
		fmt.Printf("submit amount=%d HTTP %d\n", submission.amount, resp.StatusCode)
	}

	poll := time.NewTicker(50 * time.Millisecond)
	defer poll.Stop()
	for {
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, server.URL+"/events/demo/"+event.EventID, nil)
		if err != nil {
			return err
		}
		resp, err := client.Do(req)
		if err != nil {
			return err
		}
		var record reward.Record
		err = json.NewDecoder(resp.Body).Decode(&record)
		resp.Body.Close()
		if err != nil || resp.StatusCode != http.StatusOK {
			return fmt.Errorf("read reward status HTTP %d: %v", resp.StatusCode, err)
		}
		if record.Status == "failed" {
			return fmt.Errorf("reward failed: %s", record.LastError)
		}
		if record.Status == "delivered" {
			calls, credits := publisher.Counts()
			if record.Attempts != 2 || calls != 2 || credits != 1 {
				return fmt.Errorf("unexpected attempts=%d calls=%d credits=%d", record.Attempts, calls, credits)
			}
			fmt.Printf("status=%s attempts=%d webhook_calls=%d publisher_credits=%d\n", record.Status, record.Attempts, calls, credits)
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-poll.C:
		}
	}
}
