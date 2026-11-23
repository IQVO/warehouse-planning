package memory

import (
	"context"
	"sort"
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
	// now is the clock the store reads; tests inject a moving one to age
	// stored answers without waiting a day.
	now func() time.Time
}

// NewIdempotencyStore constructs an empty IdempotencyStore on the real clock.
func NewIdempotencyStore() *IdempotencyStore {
	return NewIdempotencyStoreClock(time.Now)
}

// NewIdempotencyStoreClock constructs an empty IdempotencyStore reading now
// as its clock (time.Now when nil).
func NewIdempotencyStoreClock(now func() time.Time) *IdempotencyStore {
	if now == nil {
		now = time.Now
	}
	return &IdempotencyStore{entries: map[string]idempotencyEntry{}, now: now}
}

// Lock implements ports.IdempotencyStore.
func (s *IdempotencyStore) Lock(_ context.Context, key string) (*idempotency.Record, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	e, ok := s.entries[key]
	if !ok || s.now().Sub(e.at) > ports.IdempotencyRetention {
		return nil, nil
	}
	rec := e.rec
	return &rec, nil
}

// Save implements ports.IdempotencyStore.
func (s *IdempotencyStore) Save(_ context.Context, rec idempotency.Record) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.entries[rec.Key] = idempotencyEntry{rec: rec, at: s.now()}
	return nil
}

// PurgeExpired implements ports.IdempotencyStore: it deletes at most limit
// answers that are no longer replayed (older than IdempotencyRetention),
// oldest first, and returns how many. The same mutex serialises it with
// Lock and Save, so it never sees a half-written entry; an answer a request
// just (re)stored reads as live and stays.
func (s *IdempotencyStore) PurgeExpired(_ context.Context, limit int) (int, error) {
	if limit <= 0 {
		return 0, nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	var expired []string
	for key, e := range s.entries {
		if s.now().Sub(e.at) > ports.IdempotencyRetention {
			expired = append(expired, key)
		}
	}
	sort.Slice(expired, func(i, j int) bool { return s.entries[expired[i]].at.Before(s.entries[expired[j]].at) })
	if limit < len(expired) {
		expired = expired[:limit]
	}
	for _, key := range expired {
		delete(s.entries, key)
	}
	return len(expired), nil
}

// Savepoint implements ports.IdempotencyStore (no transaction to protect).
func (s *IdempotencyStore) Savepoint(context.Context) error { return nil }

// ReleaseSavepoint implements ports.IdempotencyStore (never aborted).
func (s *IdempotencyStore) ReleaseSavepoint(context.Context) (bool, error) { return false, nil }
