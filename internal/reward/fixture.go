package reward

import (
	"context"
	"crypto/rand"
	"database/sql"
	"errors"
	"fmt"
	"net/url"
	"time"
)

// IsolatedDatabase gives the demo and integration tests a fresh schema in a local database.
func IsolatedDatabase(ctx context.Context, dsn string) (*sql.DB, func() error, error) {
	endpoint, err := url.Parse(dsn)
	if err != nil || (endpoint.Scheme != "postgres" && endpoint.Scheme != "postgresql") {
		return nil, nil, errors.New("the isolated fixture requires a PostgreSQL URL")
	}
	var unique [12]byte
	if _, err := rand.Read(unique[:]); err != nil {
		return nil, nil, err
	}
	name := fmt.Sprintf("reward_fixture_%x", unique)
	admin, err := sql.Open("postgres", dsn)
	if err != nil {
		return nil, nil, err
	}
	if _, err := admin.ExecContext(ctx, "CREATE SCHEMA "+name); err != nil {
		admin.Close()
		return nil, nil, err
	}
	drop := func() error {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_, err := admin.ExecContext(ctx, "DROP SCHEMA "+name+" CASCADE")
		return errors.Join(err, admin.Close())
	}
	query := endpoint.Query()
	query.Set("search_path", name)
	endpoint.RawQuery = query.Encode()
	db, err := sql.Open("postgres", endpoint.String())
	if err != nil {
		return nil, nil, errors.Join(err, drop())
	}
	return db, func() error { return errors.Join(db.Close(), drop()) }, nil
}
