package memory

import (
	"context"
	"sort"
	"sync"

	"github.com/claudioed/warehouse-planning/internal/domain/pagecursor"
	"github.com/claudioed/warehouse-planning/internal/domain/processflow"
)

// ProcessFlowRepo is an in-memory, mutex-guarded ports.ProcessFlowRepository:
// one flow per flow id, replaced only by a greater version. A TEST DOUBLE only
// (ADR 0016).
type ProcessFlowRepo struct {
	mu    sync.Mutex
	store map[string]processflow.Flow
}

// NewProcessFlowRepo constructs an empty ProcessFlowRepo.
func NewProcessFlowRepo() *ProcessFlowRepo {
	return &ProcessFlowRepo{store: make(map[string]processflow.Flow)}
}

// Record implements ports.ProcessFlowRepository with the same version guard
// (Flow.Supersedes) the Postgres adapter applies in SQL.
func (r *ProcessFlowRepo) Record(_ context.Context, f processflow.Flow) (bool, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	var stored int64
	if cur, ok := r.store[f.ID()]; ok {
		stored = cur.Version()
	}
	if !f.Supersedes(stored) {
		return false, nil
	}
	r.store[f.ID()] = f
	return true, nil
}

// FindActive implements ports.ProcessFlowRepository.
func (r *ProcessFlowRepo) FindActive(_ context.Context, id string) (*processflow.Flow, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	f, ok := r.store[id]
	if !ok || !f.Active() {
		return nil, nil
	}
	return &f, nil
}

// ListActivePage implements ports.ProcessFlowRepository.
func (r *ProcessFlowRepo) ListActivePage(_ context.Context, after *pagecursor.Cursor, limit int) ([]processflow.Flow, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]processflow.Flow, 0, len(r.store))
	for _, f := range r.store {
		if f.Active() && (after == nil || f.ID() > after.ID) {
			out = append(out, f)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ID() < out[j].ID() })
	if len(out) > limit {
		out = out[:limit]
	}
	return out, nil
}

// Get returns the stored flow of an id, retired or not -- a test inspection
// helper.
func (r *ProcessFlowRepo) Get(id string) (processflow.Flow, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	f, ok := r.store[id]
	return f, ok
}

// Len is the number of stored flows -- a test inspection helper.
func (r *ProcessFlowRepo) Len() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.store)
}

// Snapshot implements Snapshotter.
func (r *ProcessFlowRepo) Snapshot() func() {
	r.mu.Lock()
	defer r.mu.Unlock()
	saved := make(map[string]processflow.Flow, len(r.store))
	for k, v := range r.store {
		saved[k] = v
	}
	return func() {
		r.mu.Lock()
		defer r.mu.Unlock()
		r.store = saved
	}
}
