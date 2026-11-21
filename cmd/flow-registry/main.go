// Command flow-registry serves Flows, their execution revisions, Source
// bindings and LLM settings over NATS request subjects, and publishes its
// audit messages and events through a PostgreSQL outbox.
package main

import (
	"context"
	"database/sql"
	"errors"
	"flag"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"sort"
	"syscall"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib"

	"github.com/pandops/flowai-flow-registry/internal/broker"
	"github.com/pandops/flowai-flow-registry/internal/config"
	"github.com/pandops/flowai-flow-registry/internal/cursor"
	"github.com/pandops/flowai-flow-registry/internal/flows"
	"github.com/pandops/flowai-flow-registry/internal/health"
	"github.com/pandops/flowai-flow-registry/internal/migrations"
	"github.com/pandops/flowai-flow-registry/internal/outbox"
	"github.com/pandops/flowai-flow-registry/internal/store"
)

func main() {
	path := flag.String("config", "/etc/flowai/config.yaml", "configuration file")
	flag.Parse()
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil)).With("service", "flow-registry")
	if err := run(*path, logger); err != nil {
		logger.Error("flow-registry stopped", "error", err)
		os.Exit(1)
	}
}

func run(path string, logger *slog.Logger) error {
	cfg, err := config.Load(path)
	if err != nil {
		return err
	}
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	db, err := sql.Open("pgx", cfg.DSN())
	if err != nil {
		return err
	}
	defer db.Close()
	if err := waitFor(ctx, logger, "postgresql", func() error { return migrations.Apply(ctx, db, cfg.DB.Schema) }); err != nil {
		return err
	}
	st := store.NewPostgres(db)

	secrets, err := cfg.CursorSecrets()
	if err != nil {
		return err
	}
	var keys []cursor.Key
	for id, secret := range secrets {
		keys = append(keys, cursor.Key{ID: id, Secret: secret})
	}
	sort.Slice(keys, func(i, j int) bool { return keys[i].ID < keys[j].ID })
	codec, err := cursor.New(keys, cfg.ActiveCursorKeyID, cfg.CursorTTL(), time.Now)
	if err != nil {
		return err
	}

	conn, err := broker.Dial(ctx, broker.DialConfig{
		Name: "flow-registry", URL: cfg.NATS.URL, CAFile: cfg.NATS.CAFile,
		CredentialsFile: cfg.NATS.CredentialsFile, InboxPrefix: cfg.NATS.InboxPrefix,
	})
	if err != nil {
		return err
	}
	defer conn.Close()

	svc := flows.New(flows.Options{Store: st, Broker: conn, Cursors: codec, Logger: logger, RequestTimeout: cfg.RequestTimeout()})
	if err := conn.Serve(svc.Subjects(), svc.Handle); err != nil {
		return err
	}
	relay := &outbox.Relay{Store: st, JS: conn, Logger: logger}
	go relay.Run(ctx)

	srv := &http.Server{
		Addr:              cfg.HTTP.Listen,
		Handler:           health.Handler(func() bool { return svc.Ready(context.Background()) }),
		ReadHeaderTimeout: 5 * time.Second,
	}
	errs := make(chan error, 1)
	go func() { errs <- srv.ListenAndServe() }()
	logger.Info("flow-registry started", "listen", cfg.HTTP.Listen)

	select {
	case <-ctx.Done():
	case err := <-errs:
		if !errors.Is(err, http.ErrServerClosed) {
			return err
		}
	}
	shutdown, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	_ = srv.Shutdown(shutdown)
	svc.Wait()
	return nil
}

// waitFor retries a startup step until it succeeds or the process stops.
func waitFor(ctx context.Context, logger *slog.Logger, what string, step func() error) error {
	for {
		err := step()
		if err == nil {
			return nil
		}
		logger.Warn("waiting for "+what, "error", err)
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(2 * time.Second):
		}
	}
}
