package memory

import (
	"context"
	"sync"

	"github.com/claudioed/warehouse-planning/internal/domain/processpath"
)

// ProcessPathRepo is an in-memory, mutex-guarded ProcessPathRepository. For
// tests only -- there is no event-driven sync from process-path-management
// yet (a later phase); this repo only supports seeding a ProcessPath
// directly via the RegisterProcessPath use case.
type ProcessPathRepo struct {
	mu    sync.Mutex
	store map[string]processpath.ProcessPath
}

// NewProcessPathRepo constructs an empty ProcessPathRepo.
func NewProcessPathRepo() *ProcessPathRepo {
	return &ProcessPathRepo{store: make(map[string]processpath.ProcessPath)}
}

// Save upserts path under its id.
func (r *ProcessPathRepo) Save(_ context.Context, path processpath.ProcessPath) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.store[path.ID()] = path
	return nil
}

// FindByID returns the stored ProcessPath for id, or (nil, nil) if nothing
// has been registered yet.
func (r *ProcessPathRepo) FindByID(_ context.Context, id string) (*processpath.ProcessPath, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	path, ok := r.store[id]
	if !ok {
		return nil, nil
	}
	return &path, nil
}
