package reward

import (
	"context"
	"database/sql"
	_ "embed"
	"errors"
	"time"
)

var ErrConflict = errors.New("event ID already exists with a different payload")

type Event struct {
	Publisher string `json:"publisher"`
	EventID   string `json:"event_id"`
	UserID    string `json:"user_id"`
	Amount    int64  `json:"amount"`
}

type Record struct {
	Event
	Status      string     `json:"status"`
	Attempts    int64      `json:"attempts"`
	NextAttempt *time.Time `json:"next_attempt_at,omitempty"`
	LastError   string     `json:"last_error,omitempty"`
}

type Store struct{ DB *sql.DB }

//go:embed schema.sql
var schema string

func (s Store) Init(ctx context.Context) error {
	// Serialize first-time initialization if two dispatchers start together.
	tx, err := s.DB.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if _, err := tx.ExecContext(ctx, "SELECT pg_advisory_xact_lock(60377419)"); err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, schema); err != nil {
		return err
	}
	return tx.Commit()
}

const recordColumns = "publisher, event_id, user_id, amount, status, attempts, next_attempt_at, last_error"

func scanRecord(row *sql.Row) (Record, error) {
	var r Record
	err := row.Scan(&r.Publisher, &r.EventID, &r.UserID, &r.Amount, &r.Status, &r.Attempts, &r.NextAttempt, &r.LastError)
	return r, err
}

func (s Store) Submit(ctx context.Context, event Event) (Record, bool, error) {
	r, err := scanRecord(s.DB.QueryRowContext(ctx, `
		INSERT INTO reward_events (publisher, event_id, user_id, amount)
		VALUES ($1, $2, $3, $4)
		ON CONFLICT (publisher, event_id) DO NOTHING
		RETURNING `+recordColumns, event.Publisher, event.EventID, event.UserID, event.Amount))
	if !errors.Is(err, sql.ErrNoRows) {
		return r, err == nil, err
	}

	// A separate statement sees a conflicting insert that just committed.
	r, err = s.Get(ctx, event.Publisher, event.EventID)
	if err == nil && r.Event != event {
		return Record{}, false, ErrConflict
	}
	return r, false, err
}

func (s Store) Get(ctx context.Context, publisher, id string) (Record, error) {
	return scanRecord(s.DB.QueryRowContext(ctx, "SELECT "+recordColumns+
		" FROM reward_events WHERE publisher = $1 AND event_id = $2", publisher, id))
}

func (s Store) Claim(ctx context.Context, lease time.Duration) (Record, error) {
	return scanRecord(s.DB.QueryRowContext(ctx, `
		UPDATE reward_events
		SET attempts = attempts + 1,
		    lease_until = CURRENT_TIMESTAMP + ($1::bigint * INTERVAL '1 microsecond')
		WHERE (publisher, event_id) = (
			SELECT publisher, event_id FROM reward_events
			WHERE status = 'pending' AND next_attempt_at <= CURRENT_TIMESTAMP
			  AND (lease_until IS NULL OR lease_until <= CURRENT_TIMESTAMP)
			ORDER BY next_attempt_at, publisher, event_id
			LIMIT 1 FOR UPDATE SKIP LOCKED
		)
		RETURNING `+recordColumns, lease.Microseconds()))
}

// Finish fences stale workers with the attempt number and the lease deadline.
func (s Store) Finish(ctx context.Context, job Record, status, reason string, delay time.Duration) (bool, error) {
	result, err := s.DB.ExecContext(ctx, `
		UPDATE reward_events
		SET status = $4, last_error = $5, lease_until = NULL,
		    next_attempt_at = CASE WHEN $4 = 'pending' THEN
		        CURRENT_TIMESTAMP + ($6::bigint * INTERVAL '1 microsecond') END
		WHERE publisher = $1 AND event_id = $2 AND attempts = $3
		  AND status = 'pending' AND lease_until > clock_timestamp()
	`, job.Publisher, job.EventID, job.Attempts, status, reason, delay.Microseconds())
	if err != nil {
		return false, err
	}
	rows, err := result.RowsAffected()
	return rows == 1, err
}
