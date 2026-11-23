package main

import (
	"context"
	"log/slog"
	"time"

	"github.com/claudioed/warehouse-planning/internal/application/ports"
)

// The Idempotency-Key retention (ADR 0026 addendum, 2026-11-23): every
// idempotencyPurgeInterval the purge deletes the key rows older than
// ports.IdempotencyRetention, at most idempotencyPurgeBatch per statement so
// one pass never holds a long lock. The two values are fixed on purpose
// (like the retention of the sibling services): the age is the store's
// IdempotencyRetention, the rest is housekeeping, not configuration.
const (
	idempotencyPurgeInterval = time.Hour
	idempotencyPurgeBatch    = 1000
)

// purgeBatch deletes one batch of expired rows and returns how many it deleted.
type purgeBatch func(ctx context.Context, limit int) (int, error)

// purgeIdempotencyKeys runs batches until one comes back short (the backlog
// is gone) or ctx ends, and returns how many rows it deleted.
func purgeIdempotencyKeys(ctx context.Context, store ports.IdempotencyStore, batch int) (int, error) {
	total := 0
	for ctx.Err() == nil {
		n, err := store.PurgeExpired(ctx, batch)
		total += n
		if err != nil || n < batch {
			return total, err
		}
	}
	return total, nil
}

// purgeLoop purges at once and then every interval until ctx ends. A failed
// pass is logged and retried at the next tick: the purge is idempotent, and
// a row that outlives its retention is never replayed (the window is
// enforced at read), so a failure costs only disk.
func purgeLoop(ctx context.Context, logger *slog.Logger, store ports.IdempotencyStore, interval time.Duration, batch int) {
	t := time.NewTicker(interval)
	defer t.Stop()
	for {
		switch n, err := purgeIdempotencyKeys(ctx, store, batch); {
		case err != nil && ctx.Err() == nil:
			logger.Error("idempotency purge failed", "deleted", n, "error", err)
		case n > 0:
			logger.Info("idempotency purge done", "deleted", n)
		}
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
	}
}

// startIdempotencyPurge starts the always-on retention loop (the same
// worker supervision the consumers and the outbox relay use: a purge that
// returns early is a dead worker and exits the process).
func startIdempotencyPurge(store ports.IdempotencyStore, logger *slog.Logger) (*worker, func()) {
	ctx, stop := context.WithCancel(context.Background())
	w := &worker{name: "idempotency-purge", done: make(chan struct{})}
	go func() {
		defer close(w.done)
		logger.Info("idempotency purge running", "interval", idempotencyPurgeInterval, "batch", idempotencyPurgeBatch, "retention", ports.IdempotencyRetention)
		purgeLoop(ctx, logger, store, idempotencyPurgeInterval, idempotencyPurgeBatch)
	}()
	return w, stop
}
