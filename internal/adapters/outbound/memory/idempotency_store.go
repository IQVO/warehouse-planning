package memory

import (
	"context"
	"sync"
	"time"

	"github.com/claudioed/warehouse-planning/internal/domain/idempotency"

	"github.com/claudioed/warehouse-planning/internal/application/ports"
)

type idempotencyEntry struct {
	rec idempotency.Record
	at  time.Time
}

// IdempotencyStore is the in-memory ports.IdempotencyStore test double.
// Same-key requests are serialised by the (coarse) in-memory UnitOfWork the
// middleware runs inside, so Lock only reads.
type IdempotencyStore struct {
	mu      sync.Mutex
	entries map[string]idempotencyEntry
}

// NewIdempotencyStore constructs an empty IdempotencyStore.
func NewIdempotencyStore() *IdempotencyStore {
	return &IdempotencyStore{entries: map[string]idempotencyEntry{}}
}

// Lock implements ports.IdempotencyStore.
func (s *IdempotencyStore) Lock(_ context.Context, key string) (*idempotency.Record, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	e, ok := s.entries[key]
	if !ok || time.Since(e.at) > ports.IdempotencyRetention {
		return nil, nil
	}
	rec := e.rec
	return &rec, nil
}

// Save implements ports.IdempotencyStore.
func (s *IdempotencyStore) Save(_ context.Context, rec idempotency.Record) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.entries[rec.Key] = idempotencyEntry{rec: rec, at: time.Now()}
	return nil
}
