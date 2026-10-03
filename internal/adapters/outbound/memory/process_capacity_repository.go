// Package memory provides in-memory, mutex-guarded outbound adapter
// implementations for tests and local dev. Never wired into production --
// see cmd/'s composition root for the real (Postgres-backed) wiring.
package memory

import (
	"context"
	"sync"
	"time"

	"github.com/claudioed/warehouse-planning/internal/domain/processcapacity"
)

// processCapacityKey is the composite identity ProcessCapacityRepo keys on:
// (ProcessType, Location, CapacityWindow). Window identity is by exact
// start/end instant, matching the aggregate's own identity rule.
type processCapacityKey struct {
	processType processcapacity.ProcessType
	location    string
	windowStart int64 // UnixNano, for a comparable map key
	windowEnd   int64
}

// ProcessCapacityRepo is an in-memory, mutex-guarded ProcessCapacityRepository.
// For tests only.
type ProcessCapacityRepo struct {
	mu    sync.Mutex
	store map[processCapacityKey]*processcapacity.ProcessCapacity
}

// NewProcessCapacityRepo constructs an empty ProcessCapacityRepo.
func NewProcessCapacityRepo() *ProcessCapacityRepo {
	return &ProcessCapacityRepo{store: make(map[processCapacityKey]*processcapacity.ProcessCapacity)}
}

func keyFor(processType processcapacity.ProcessType, location string, windowStart, windowEnd int64) processCapacityKey {
	return processCapacityKey{processType: processType, location: location, windowStart: windowStart, windowEnd: windowEnd}
}

// Save upserts pc under its identity key.
func (r *ProcessCapacityRepo) Save(_ context.Context, pc *processcapacity.ProcessCapacity) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	k := keyFor(pc.ProcessType(), pc.Location(), pc.Window().Start().UnixNano(), pc.Window().End().UnixNano())
	r.store[k] = clonePC(pc)
	return nil
}

// clonePC deep-copies pc by replaying its constraints, the same way the
// Postgres adapter rebuilds an aggregate from rows. Storing and returning
// copies (never the caller's pointer) makes this double behave like a real
// store: mutating an aggregate after Find has no effect until Save.
func clonePC(pc *processcapacity.ProcessCapacity) *processcapacity.ProcessCapacity {
	cp := processcapacity.NewProcessCapacity(pc.ProcessType(), pc.Location(), pc.Window())
	for _, entry := range pc.Constraints() {
		// Cannot fail: pc's constraints already share one native unit.
		_ = cp.AddConstraint(entry.Type, entry.Rate)
	}
	return cp
}

// Snapshot implements Snapshotter.
func (r *ProcessCapacityRepo) Snapshot() func() {
	r.mu.Lock()
	defer r.mu.Unlock()
	saved := make(map[processCapacityKey]*processcapacity.ProcessCapacity, len(r.store))
	for k, pc := range r.store {
		saved[k] = clonePC(pc)
	}
	return func() {
		r.mu.Lock()
		defer r.mu.Unlock()
		r.store = saved
	}
}

// FindByProcessLocationWindow returns the stored ProcessCapacity for the
// given identity, or (nil, nil) if nothing has been registered yet.
func (r *ProcessCapacityRepo) FindByProcessLocationWindow(
	_ context.Context,
	processType processcapacity.ProcessType,
	location string,
	windowStart, windowEnd time.Time,
) (*processcapacity.ProcessCapacity, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	k := keyFor(processType, location, windowStart.UnixNano(), windowEnd.UnixNano())
	pc, ok := r.store[k]
	if !ok {
		return nil, nil
	}
	return clonePC(pc), nil
}
