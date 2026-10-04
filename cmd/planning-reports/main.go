// Command planning-reports is the read-only HTTP reader of the
// warehouse-planning analytics read side (ADR 0005): GET
// /reports/{bottleneck-frequency,shortage-trend,plan-throughput} and
// /healthz on :8092, answered from the ANALYTICAL database only (a
// read-only pool; the database role should be read-only too). It writes
// nothing and never opens the OLTP database or Kafka. No auth layer: access
// control is the in-cluster boundary (fleet rule).
package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	inboundhttp "github.com/claudioed/warehouse-planning/internal/adapters/inbound/http"
	"github.com/claudioed/warehouse-planning/internal/adapters/outbound/analyticsstore"
	"github.com/claudioed/warehouse-planning/internal/bootretry"
)

const shutdownTimeout = 10 * time.Second

var errMissingAnalyticsURL = errors.New("ANALYTICS_DATABASE_URL is required: the reports are served from the analytical database")

func main() {
	if err := run(); err != nil {
		slog.Error("planning-reports exited with error", "error", err)
		os.Exit(1)
	}
}

func run() error {
	logger := newLogger(getenv("LOG_LEVEL", "info"))
	slog.SetDefault(logger)

	url := os.Getenv("ANALYTICS_DATABASE_URL")
	if url == "" {
		return errMissingAnalyticsURL
	}
	httpAddr := getenv("HTTP_ADDR", ":8092")

	ctx := context.Background()
	pool, err := analyticsstore.NewReadOnlyPool(ctx, url)
	if err != nil {
		return err
	}
	defer pool.Close()
	// Retried: the first outbound dial of an injected pod is reset ~10s
	// after start (Istio native sidecars).
	if err := bootretry.Retry(ctx, logger, "ping analytics database", func() error { return pool.Ping(ctx) }); err != nil {
		return err
	}

	srv := &http.Server{
		Addr:              httpAddr,
		Handler:           inboundhttp.NewReportsRouter(&inboundhttp.ReportsServer{Reader: analyticsstore.NewReader(pool)}),
		ReadHeaderTimeout: 5 * time.Second,
	}
	serveErr := make(chan error, 1)
	go func() {
		logger.Info("reports server listening", "addr", httpAddr)
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			serveErr <- err
		}
	}()

	sig, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	select {
	case err := <-serveErr:
		return fmt.Errorf("reports server failed: %w", err)
	case <-sig.Done():
	}

	shutdownCtx, cancel := context.WithTimeout(context.Background(), shutdownTimeout)
	defer cancel()
	return srv.Shutdown(shutdownCtx)
}

func newLogger(level string) *slog.Logger {
	var lvl slog.Level
	switch strings.ToLower(level) {
	case "debug":
		lvl = slog.LevelDebug
	case "warn", "warning":
		lvl = slog.LevelWarn
	case "error":
		lvl = slog.LevelError
	default:
		lvl = slog.LevelInfo
	}
	return slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: lvl}))
}

func getenv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
