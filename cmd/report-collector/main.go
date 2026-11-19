package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"net/http/httptest"
	"os"
	"os/signal"
	"syscall"
	"time"

	"adtech-portfolio/internal/report"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if err := run(ctx); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run(ctx context.Context) error {
	day := flag.String("date", "2026-01-15", "UTC report date (YYYY-MM-DD)")
	output := flag.String("out", "snapshots", "snapshot output directory")
	limit := flag.Int("concurrency", 2, "maximum concurrent report requests")
	fail := flag.Bool("fail-second-page", false, "fail the cents mock after its first page")
	csvURL := flag.String("csv-url", "", "CSV report endpoint")
	centsURL := flag.String("cents-url", "", "paginated cents report endpoint")
	microsURL := flag.String("micros-url", "", "paginated micros report endpoint")
	flag.Parse()

	if *csvURL == "" && *centsURL == "" && *microsURL == "" {
		csv := httptest.NewServer(report.MockReport(report.CSVDollars, false))
		defer csv.Close()
		cents := httptest.NewServer(report.MockReport(report.JSONCents, *fail))
		defer cents.Close()
		micros := httptest.NewServer(report.MockReport(report.JSONMicros, false))
		defer micros.Close()
		*csvURL, *centsURL, *microsURL = csv.URL, cents.URL, micros.URL
	} else if *fail || *csvURL == "" || *centsURL == "" || *microsURL == "" {
		return errors.New("provide all three report URLs, or omit them to use local mocks")
	}
	ctx, cancel := context.WithTimeout(ctx, time.Minute)
	defer cancel()
	results, err := report.Collect(ctx, *day, *output, report.MockProviders(*csvURL, *centsURL, *microsURL), *limit)
	for _, result := range results {
		if result.Err != nil {
			fmt.Printf("%s failed: %v\n", result.Provider, result.Err)
			continue
		}
		fmt.Printf("%s published impressions=%d revenue_usd_micros=%d\n",
			result.Provider, result.Total.Impressions, result.Total.USDMicros)
	}
	return err
}
