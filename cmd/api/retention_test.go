package main

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/claudioed/warehouse-planning/internal/application/ports"
	"github.com/claudioed/warehouse-planning/internal/domain/idempotency"
)

var _ ports.IdempotencyStore = (*scriptedPurgeStore)(nil)

// scriptedPurgeStore answers PurgeExpired with one scripted batch per call
// (then 0) and counts the calls; the rest of the port is a no-op.
type scriptedPurgeStore struct {
	mu      sync.Mutex
	batches []int
	err     error
	calls   int
}

func (f *scriptedPurgeStore) PurgeExpired(context.Context, int) (int, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.calls < len(f.batches) {
		n := f.batches[f.calls]
		f.calls++
		return n, nil
	}
	f.calls++
	return 0, f.err
}

func (*scriptedPurgeStore) Lock(context.Context, string) (*idempotency.Record, error) {
	return nil, nil
}
func (*scriptedPurgeStore) Save(context.Context, idempotency.Record) error { return nil }
func (*scriptedPurgeStore) Savepoint(context.Context) error                { return nil }
func (*scriptedPurgeStore) ReleaseSavepoint(context.Context) (bool, error) { return false, nil }

func (f *scriptedPurgeStore) callsMade() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.calls
}

func TestThePurgeRunsBatchesUntilOneComesBackShort(t *testing.T) {
	store := &scriptedPurgeStore{batches: []int{500, 500, 100}}
	total, err := purgeIdempotencyKeys(context.Background(), store, 500)
	if err != nil || total != 1100 || store.callsMade() != 3 {
		t.Fatalf("purged %d in %d calls (%v), want 1100 in 3 batches: a short batch ends the pass", total, store.callsMade(), err)
	}
}

func TestAFailedPurgeEndsThePass(t *testing.T) {
	store := &scriptedPurgeStore{batches: []int{2}, err: errors.New("db down")}
	total, err := purgeIdempotencyKeys(context.Background(), store, 2)
	if total != 2 || err == nil || store.callsMade() != 2 {
		t.Fatalf("purged %d in %d calls (%v), want 2 and the pass ended after the failure", total, store.callsMade(), err)
	}
}

func TestACancelledPurgeDeletesNothing(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	store := &scriptedPurgeStore{batches: []int{5}}
	if total, err := purgeIdempotencyKeys(ctx, store, 10); total != 0 || err != nil || store.callsMade() != 0 {
		t.Fatalf("a cancelled purge = %d %v in %d calls, want a clean no-op", total, err, store.callsMade())
	}
}

func TestThePurgeLoopLogsAFailedPassAndStopsOnCancel(t *testing.T) {
	var logs bytes.Buffer
	logger := slog.New(slog.NewTextHandler(&logs, nil))
	store := &scriptedPurgeStore{err: errors.New("db down")}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { defer close(done); purgeLoop(ctx, logger, store, time.Hour, 10) }()
	// The first pass runs at once and fails; the loop must log it and wait
	// for the next tick, not exit (only the worker's stop ends it).
	deadline := time.After(5 * time.Second)
	for !strings.Contains(logs.String(), "idempotency purge failed") {
		select {
		case <-deadline:
			t.Fatalf("the failed pass is logged: %q", logs.String())
		case <-time.After(5 * time.Millisecond):
		}
	}
	select {
	case <-done:
		t.Fatal("the loop exited on a failed pass: a failure is retried at the next tick")
	default:
	}
	cancel()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("the loop did not stop when cancelled")
	}
}

func TestThePurgeWorkerStopsWhenItsStopIsCalled(t *testing.T) {
	store := &scriptedPurgeStore{}
	w, stop := startIdempotencyPurge(store, slog.New(slog.NewTextHandler(&bytes.Buffer{}, nil)))
	stop()
	select {
	case <-w.done:
	case <-time.After(5 * time.Second):
		t.Fatal("the purge worker did not stop")
	}
}
