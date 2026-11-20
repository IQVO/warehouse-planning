package memory

import (
	"context"
	"sync"
	"time"

	"github.com/claudioed/warehouse-planning/internal/domain/laborstandard"
)

// LaborStandardRepo is an in-memory, mutex-guarded
// ports.LaborStandardRepository: one record per labor-performance standard id.
// A TEST DOUBLE only (ADR 0016).
type LaborStandardRepo struct {
	mu    sync.Mutex
	store map[string]laborstandard.Standard
}

// NewLaborStandardRepo constructs an empty LaborStandardRepo.
func NewLaborStandardRepo() *LaborStandardRepo {
	return &LaborStandardRepo{store: make(map[string]laborstandard.Standard)}
}

// Record implements ports.LaborStandardRepository: a standard id already
// stored is a no-op (the first record of an id is kept, like the Postgres
// adapter's ON CONFLICT DO NOTHING).
func (r *LaborStandardRepo) Record(_ context.Context, s laborstandard.Standard) (bool, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, ok := r.store[s.ID()]; ok {
		return false, nil
	}
	r.store[s.ID()] = s
	return true, nil
}

// InForce implements ports.LaborStandardRepository with the same rule
// (laborstandard.Resolve) the Postgres adapter applies.
func (r *LaborStandardRepo) InForce(_ context.Context, siteCode, taskType string, windowStart time.Time) (laborstandard.Standard, bool, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	all := make([]laborstandard.Standard, 0, len(r.store))
	for _, s := range r.store {
		all = append(all, s)
	}
	s, ok := laborstandard.Resolve(all, siteCode, taskType, windowStart)
	return s, ok, nil
}

// Get returns the stored record of a standard id -- a test inspection helper.
func (r *LaborStandardRepo) Get(id string) (laborstandard.Standard, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	s, ok := r.store[id]
	return s, ok
}

// Len is the number of stored records -- a test inspection helper.
func (r *LaborStandardRepo) Len() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.store)
}

// Snapshot implements Snapshotter.
func (r *LaborStandardRepo) Snapshot() func() {
	r.mu.Lock()
	defer r.mu.Unlock()
	saved := make(map[string]laborstandard.Standard, len(r.store))
	for k, v := range r.store {
		saved[k] = v
	}
	return func() {
		r.mu.Lock()
		defer r.mu.Unlock()
		r.store = saved
	}
}
