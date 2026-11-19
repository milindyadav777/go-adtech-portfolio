package reward

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"io"
	"log"
	"net/http"
	"regexp"
	"time"
)

var identifier = regexp.MustCompile(`^[a-zA-Z0-9_-]{1,80}$`)

func API(store Store, partners map[string]Partner) http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("POST /events", func(w http.ResponseWriter, r *http.Request) {
		var event Event
		input := json.NewDecoder(http.MaxBytesReader(w, r.Body, 4096))
		input.DisallowUnknownFields()
		if err := input.Decode(&event); err != nil || input.Decode(new(any)) != io.EOF ||
			!identifier.MatchString(event.EventID) || !identifier.MatchString(event.UserID) ||
			!identifier.MatchString(event.Publisher) || event.Amount <= 0 {
			http.Error(w, "invalid reward event", http.StatusBadRequest)
			return
		}
		if _, known := partners[event.Publisher]; !known {
			http.Error(w, "unknown publisher", http.StatusBadRequest)
			return
		}
		ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
		defer cancel()
		record, created, err := store.Submit(ctx, event)
		switch {
		case errors.Is(err, ErrConflict):
			http.Error(w, err.Error(), http.StatusConflict)
		case err != nil:
			databaseError(w, err)
		case created:
			writeJSON(w, http.StatusAccepted, record)
		default:
			writeJSON(w, http.StatusOK, record)
		}
	})
	mux.HandleFunc("GET /events/{publisher}/{eventID}", func(w http.ResponseWriter, r *http.Request) {
		ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
		defer cancel()
		record, err := store.Get(ctx, r.PathValue("publisher"), r.PathValue("eventID"))
		switch {
		case errors.Is(err, sql.ErrNoRows):
			http.NotFound(w, r)
		case err != nil:
			databaseError(w, err)
		default:
			writeJSON(w, http.StatusOK, record)
		}
	})
	return mux
}

func databaseError(w http.ResponseWriter, err error) {
	log.Printf("reward database: %v", err)
	http.Error(w, "database unavailable", http.StatusServiceUnavailable)
}

func writeJSON(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	if err := json.NewEncoder(w).Encode(value); err != nil {
		log.Printf("write JSON response: %v", err)
	}
}
