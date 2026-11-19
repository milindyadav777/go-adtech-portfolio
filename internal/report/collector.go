package report

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"strings"
	"time"
)

type Format int

const (
	CSVDollars Format = iota
	JSONCents
	JSONMicros
)

type Provider struct {
	Name, URL   string
	Format      Format
	MinInterval time.Duration
}

type Totals struct {
	Impressions int64 `json:"impressions"`
	USDMicros   int64 `json:"revenue_usd_micros"`
}

type Snapshot struct {
	Provider   string            `json:"provider"`
	Date       string            `json:"date"`
	Publishers map[string]Totals `json:"publishers"`
	Total      Totals            `json:"total"`
}

type Result struct {
	Provider string
	Total    Totals
	Err      error
}

var providerName = regexp.MustCompile(`^[a-zA-Z0-9_-]{1,64}$`)

func Collect(ctx context.Context, day, dir string, providers []Provider, limit int) ([]Result, error) {
	if _, err := time.Parse(time.DateOnly, day); err != nil {
		return nil, fmt.Errorf("invalid report date: %w", err)
	}
	if limit < 1 || len(providers) == 0 {
		return nil, errors.New("at least one provider and one concurrency slot are required")
	}
	names := make(map[string]bool)
	for _, p := range providers {
		if !providerName.MatchString(p.Name) || names[p.Name] || p.MinInterval <= 0 ||
			p.Format < CSVDollars || p.Format > JSONMicros {
			return nil, fmt.Errorf("invalid or duplicate provider %q", p.Name)
		}
		names[p.Name] = true
	}
	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, err
	}

	client := &http.Client{
		CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse },
	}
	defer client.CloseIdleConnections()
	slots := make(chan struct{}, limit)
	results := make(chan Result, len(providers))
	for _, p := range providers {
		go func(p Provider) {
			snapshot, err := collectProvider(ctx, client, slots, p, day)
			if err == nil {
				err = publish(ctx, dir, snapshot)
			}
			results <- Result{Provider: p.Name, Total: snapshot.Total, Err: err}
		}(p)
	}

	all := make([]Result, 0, len(providers))
	for range providers {
		all = append(all, <-results)
	}
	slices.SortFunc(all, func(a, b Result) int { return strings.Compare(a.Provider, b.Provider) })
	var failures []error
	for _, result := range all {
		if result.Err != nil {
			failures = append(failures, fmt.Errorf("%s: %w", result.Provider, result.Err))
		}
	}
	return all, errors.Join(failures...)
}

type jsonRow struct {
	Publisher   string `json:"publisher"`
	Impressions *int64 `json:"impressions"`
	Revenue     *int64 `json:"revenue"`
}

type jsonPage struct {
	Rows []jsonRow `json:"rows"`
	Next string    `json:"next_cursor,omitempty"`
}

func collectProvider(ctx context.Context, client *http.Client, slots chan struct{}, p Provider, day string) (Snapshot, error) {
	snapshot := Snapshot{Provider: p.Name, Date: day, Publishers: make(map[string]Totals)}
	var cursor string
	var lastStart time.Time
	seen := make(map[string]bool)
	for {
		if err := wait(ctx, time.Until(lastStart.Add(p.MinInterval))); err != nil {
			return Snapshot{}, err
		}
		next, err := fetchPage(ctx, client, slots, p, cursor, &lastStart, &snapshot)
		if err != nil {
			return Snapshot{}, err
		}
		if next == "" {
			return snapshot, nil
		}
		if seen[next] || len(seen) >= 999 {
			return Snapshot{}, errors.New("pagination loop or page limit exceeded")
		}
		seen[next] = true
		cursor = next
	}
}

func wait(ctx context.Context, delay time.Duration) error {
	if delay <= 0 {
		return ctx.Err()
	}
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return ctx.Err()
	}
}

func fetchPage(ctx context.Context, client *http.Client, slots chan struct{}, p Provider, cursor string, lastStart *time.Time, snapshot *Snapshot) (string, error) {
	endpoint, err := url.Parse(p.URL)
	if err != nil {
		return "", err
	}
	query := endpoint.Query()
	query.Set("date", snapshot.Date)
	if cursor == "" {
		query.Del("cursor")
	} else {
		query.Set("cursor", cursor)
	}
	endpoint.RawQuery = query.Encode()

	// Keep the global slot until the response body has been consumed and closed.
	select {
	case slots <- struct{}{}:
		defer func() { <-slots }()
	case <-ctx.Done():
		return "", ctx.Err()
	}
	pageCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(pageCtx, http.MethodGet, endpoint.String(), nil)
	if err != nil {
		return "", err
	}
	*lastStart = time.Now()
	resp, err := client.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("report endpoint returned HTTP %d", resp.StatusCode)
	}
	if p.Format == CSVDollars {
		return "", ParseCSV(pageCtx, resp.Body, snapshot)
	}

	var page jsonPage
	body := &io.LimitedReader{R: resp.Body, N: 4<<20 + 1}
	input := json.NewDecoder(body)
	if err := input.Decode(&page); err != nil {
		return "", err
	}
	if input.Decode(new(any)) != io.EOF || body.N == 0 || page.Rows == nil {
		return "", errors.New("invalid or oversized JSON report page")
	}
	scale := int64(1)
	if p.Format == JSONCents {
		scale = 10_000
	}
	for _, row := range page.Rows {
		if err := pageCtx.Err(); err != nil {
			return "", err
		}
		if row.Revenue == nil || *row.Revenue < 0 || *row.Revenue > math.MaxInt64/scale || row.Impressions == nil {
			return "", errors.New("invalid JSON impressions or revenue")
		}
		if err := snapshot.add(row.Publisher, *row.Impressions, *row.Revenue*scale); err != nil {
			return "", err
		}
	}
	return page.Next, nil
}

func (s *Snapshot) add(publisher string, impressions, micros int64) error {
	if publisher == "" || impressions < 0 || micros < 0 ||
		impressions > math.MaxInt64-s.Total.Impressions || micros > math.MaxInt64-s.Total.USDMicros {
		return errors.New("invalid metrics or snapshot total overflow")
	}
	totals := s.Publishers[publisher]
	totals.Impressions += impressions
	totals.USDMicros += micros
	s.Publishers[publisher] = totals
	s.Total.Impressions += impressions
	s.Total.USDMicros += micros
	return nil
}

func publish(ctx context.Context, dir string, snapshot Snapshot) error {
	file, err := os.CreateTemp(dir, ".report-*")
	if err != nil {
		return err
	}
	defer os.Remove(file.Name())
	defer file.Close()
	if err := json.NewEncoder(file).Encode(snapshot); err != nil {
		return err
	}
	if err := file.Sync(); err != nil {
		return err
	}
	if err := file.Close(); err != nil {
		return err
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	// A same-directory rename makes the complete snapshot visible in one step.
	return os.Rename(file.Name(), filepath.Join(dir, snapshot.Provider+"-"+snapshot.Date+".json"))
}
