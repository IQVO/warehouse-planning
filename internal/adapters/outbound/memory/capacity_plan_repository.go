package memory

import (
	"context"
	"sort"
	"sync"

	"github.com/claudioed/warehouse-planning/internal/domain/capacityplan"
)

// CapacityPlanRepo is an in-memory, mutex-guarded
// ports.CapacityPlanRepository. For tests and local dev only.
type CapacityPlanRepo struct {
	mu    sync.Mutex
	store map[string]capacityplan.RehydrateParams
}

// NewCapacityPlanRepo constructs an empty CapacityPlanRepo.
func NewCapacityPlanRepo() *CapacityPlanRepo {
	return &CapacityPlanRepo{store: make(map[string]capacityplan.RehydrateParams)}
}

func stateOf(p *capacityplan.CapacityPlan) capacityplan.RehydrateParams {
	return capacityplan.RehydrateParams{
		ID:                 p.ID(),
		WarehouseID:        p.WarehouseID(),
		Location:           p.Location(),
		Window:             p.Window(),
		ProcessPathID:      p.ProcessPathID(),
		AssignedDemand:     p.AssignedDemand(),
		PathCapacity:       p.PathCapacity(),
		BottleneckStep:     p.BottleneckStep(),
		CapacityOverWindow: p.CapacityOverWindow(),
		Shortage:           p.Shortage(),
		Status:             p.Status(),
		CreatedAt:          p.CreatedAt(),
		PublishedAt:        p.PublishedAt(),

		DemandSource: p.DemandSource(),

		BottleneckConstraint: p.BottleneckConstraint(),
		Warnings:             p.Warnings(),
	}
}

// Save upserts plan's state under its id. Like the Postgres adapter it
// stores state, never the pending domain events, and returns fresh copies
// from FindByID.
func (r *CapacityPlanRepo) Save(_ context.Context, plan *capacityplan.CapacityPlan) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.store[plan.ID()] = stateOf(plan)
	return nil
}

// FindByID returns a copy of the stored plan, or (nil, nil).
func (r *CapacityPlanRepo) FindByID(_ context.Context, id string) (*capacityplan.CapacityPlan, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	state, ok := r.store[id]
	if !ok {
		return nil, nil
	}
	return capacityplan.Rehydrate(state), nil
}

// ListRecent returns up to limit plans, newest first (created_at, then id,
// both descending), optionally restricted to one location. Like FindByID it
// returns fresh copies.
func (r *CapacityPlanRepo) ListRecent(_ context.Context, location string, limit int) ([]*capacityplan.CapacityPlan, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	matching := make([]capacityplan.RehydrateParams, 0, len(r.store))
	for _, state := range r.store {
		if location == "" || state.Location == location {
			matching = append(matching, state)
		}
	}
	sort.Slice(matching, func(i, j int) bool {
		if !matching[i].CreatedAt.Equal(matching[j].CreatedAt) {
			return matching[i].CreatedAt.After(matching[j].CreatedAt)
		}
		return matching[i].ID > matching[j].ID
	})
	if limit >= 0 && len(matching) > limit {
		matching = matching[:limit]
	}
	out := make([]*capacityplan.CapacityPlan, 0, len(matching))
	for _, state := range matching {
		out = append(out, capacityplan.Rehydrate(state))
	}
	return out, nil
}

// Snapshot implements Snapshotter.
func (r *CapacityPlanRepo) Snapshot() func() {
	r.mu.Lock()
	defer r.mu.Unlock()
	saved := make(map[string]capacityplan.RehydrateParams, len(r.store))
	for k, v := range r.store {
		saved[k] = v
	}
	return func() {
		r.mu.Lock()
		defer r.mu.Unlock()
		r.store = saved
	}
}
