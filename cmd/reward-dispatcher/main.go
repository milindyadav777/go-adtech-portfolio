package main

import (
	"context"
	"database/sql"
	"errors"
	"flag"
	"log"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"os/signal"
	"syscall"
	"time"

	_ "github.com/lib/pq"

	"adtech-portfolio/internal/reward"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if err := run(ctx); err != nil {
		log.Print(err)
		os.Exit(1)
	}
}

func run(ctx context.Context) (err error) {
	addr := flag.String("listen", "127.0.0.1:8080", "HTTP API listen address")
	target := flag.String("webhook-url", "", "local webhook URL for publisher demo")
	demo := flag.Bool("demo", false, "run an end-to-end local demo with a dropped webhook reply")
	flag.Parse()

	dsn := os.Getenv("DATABASE_URL")
	if dsn == "" {
		return errors.New("set DATABASE_URL to a PostgreSQL connection string")
	}
	startup, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	var db *sql.DB
	if *demo {
		var cleanup func() error
		db, cleanup, err = reward.IsolatedDatabase(startup, dsn)
		if err != nil {
			return err
		}
		defer func() { err = errors.Join(err, cleanup()) }()
	} else {
		db, err = sql.Open("postgres", dsn)
		if err != nil {
			return err
		}
		defer db.Close()
	}
	db.SetMaxOpenConns(8)
	store := reward.Store{DB: db}
	if err := store.Init(startup); err != nil {
		return err
	}

	partner := reward.Partner{URL: *target, Secret: os.Getenv("WEBHOOK_SECRET")}
	timeout := 2 * time.Second
	var mock *reward.MockPublisher
	if *demo {
		mock = &reward.MockPublisher{Secret: "local-demo-secret", LoseFirstReply: true}
		publisher := httptest.NewServer(mock)
		defer publisher.Close()
		partner = reward.Partner{URL: publisher.URL, Secret: mock.Secret}
		timeout = 350 * time.Millisecond
	}
	endpoint, urlErr := url.Parse(partner.URL)
	if urlErr != nil || endpoint.Host == "" || (endpoint.Scheme != "http" && endpoint.Scheme != "https") || partner.Secret == "" {
		return errors.New("provide an HTTP webhook URL and WEBHOOK_SECRET, or run with -demo")
	}
	partners := map[string]reward.Partner{"demo": partner}
	dispatcher := reward.Dispatcher{
		Store: store, Partners: partners, Workers: 3, Timeout: timeout, Lease: 10 * time.Second,
		Poll: 50 * time.Millisecond, MinBackoff: 150 * time.Millisecond, MaxBackoff: 10 * time.Second,
	}
	workerCtx, stopWorkers := context.WithCancel(ctx)
	workers := make(chan error, 1)
	go func() { workers <- dispatcher.Run(workerCtx) }()
	defer func() {
		stopWorkers()
		err = errors.Join(err, <-workers)
	}()

	api := reward.API(store, partners)
	if *demo {
		return runDemo(ctx, api, mock)
	}
	return serve(ctx, *addr, api)
}

func serve(ctx context.Context, addr string, api http.Handler) error {
	server := &http.Server{
		Addr: addr, Handler: api, ReadHeaderTimeout: 2 * time.Second,
		ReadTimeout: 5 * time.Second, WriteTimeout: 5 * time.Second, IdleTimeout: 30 * time.Second,
	}
	done := make(chan error, 1)
	go func() { done <- server.ListenAndServe() }()
	select {
	case err := <-done:
		return err
	case <-ctx.Done():
		shutdown, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := server.Shutdown(shutdown); err != nil {
			return errors.Join(err, server.Close())
		}
		return nil
	}
}
