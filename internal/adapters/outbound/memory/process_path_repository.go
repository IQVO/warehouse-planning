package memory

import (
	"context"
	"sort"
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

// List returns every stored ProcessPath ordered by id (empty, non-nil when
// none is stored).
func (r *ProcessPathRepo) List(_ context.Context) ([]processpath.ProcessPath, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]processpath.ProcessPath, 0, len(r.store))
	for _, path := range r.store {
		out = append(out, path)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ID() < out[j].ID() })
	return out, nil
}
