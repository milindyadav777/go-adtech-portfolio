package report

import (
	"encoding/json"
	"fmt"
	"net/http"
	"time"
)

func MockProviders(csvURL, centsURL, microsURL string) []Provider {
	return []Provider{
		{Name: "csv", URL: csvURL, Format: CSVDollars, MinInterval: 25 * time.Millisecond},
		{Name: "cents", URL: centsURL, Format: JSONCents, MinInterval: 40 * time.Millisecond},
		{Name: "micros", URL: microsURL, Format: JSONMicros, MinInterval: 60 * time.Millisecond},
	}
}

func MockReport(format Format, failSecondPage bool) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if _, err := time.Parse(time.DateOnly, r.URL.Query().Get("date")); err != nil {
			http.Error(w, "invalid date", http.StatusBadRequest)
			return
		}
		if format == CSVDollars {
			w.Header().Set("Content-Type", "text/csv")
			fmt.Fprint(w, "publisher,impressions,revenue_usd\nalpha,100,1.125000\nalpha,20,0.000001\nbeta,50,0.2\n")
			return
		}

		var publisher, next string
		var impressions, revenue int64
		switch r.URL.Query().Get("cursor") {
		case "":
			publisher, impressions, revenue, next = "alpha", 75, 125, "second"
		case "second":
			if failSecondPage {
				http.Error(w, "mock second page failure", http.StatusServiceUnavailable)
				return
			}
			publisher, impressions, revenue = "beta", 25, 25
		default:
			http.Error(w, "unknown cursor", http.StatusBadRequest)
			return
		}
		if format == JSONMicros {
			revenue *= 1_000
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(jsonPage{
			Rows: []jsonRow{{Publisher: publisher, Impressions: &impressions, Revenue: &revenue}},
			Next: next,
		})
	})
}
