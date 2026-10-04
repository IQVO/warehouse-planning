package memory

import (
	"context"
	"sync"
	"time"

	"github.com/claudioed/warehouse-planning/internal/domain/demand"
)

// OrderDemandRepo is an in-memory, mutex-guarded ports.OrderDemandRepository.
// For tests and local dev only.
type OrderDemandRepo struct {
	mu     sync.Mutex
	orders map[string]demand.Order
}

// NewOrderDemandRepo constructs an empty OrderDemandRepo.
func NewOrderDemandRepo() *OrderDemandRepo {
	return &OrderDemandRepo{orders: make(map[string]demand.Order)}
}

// Upsert implements ports.OrderDemandRepository with the same
// last-writer-wins rule (demand.Order.Supersedes) the Postgres adapter
// applies.
func (r *OrderDemandRepo) Upsert(_ context.Context, o demand.Order) (bool, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if prev, ok := r.orders[o.ID()]; ok && !o.Supersedes(prev) {
		return false, nil
	}
	r.orders[o.ID()] = o
	return true, nil
}

// Expected implements ports.OrderDemandRepository.
func (r *OrderDemandRepo) Expected(_ context.Context, location string, start, end time.Time) (demand.Summary, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	site := make([]demand.Order, 0, len(r.orders))
	for _, o := range r.orders {
		if o.Location() == location {
			site = append(site, o)
		}
	}
	return demand.Summarize(site, start, end), nil
}

// Get returns the stored order for id -- a test inspection helper.
func (r *OrderDemandRepo) Get(id string) (demand.Order, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	o, ok := r.orders[id]
	return o, ok
}

// Len is the number of stored orders -- a test inspection helper.
func (r *OrderDemandRepo) Len() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.orders)
}

// Snapshot implements Snapshotter.
func (r *OrderDemandRepo) Snapshot() func() {
	r.mu.Lock()
	defer r.mu.Unlock()
	saved := make(map[string]demand.Order, len(r.orders))
	for k, v := range r.orders {
		saved[k] = v
	}
	return func() {
		r.mu.Lock()
		defer r.mu.Unlock()
		r.orders = saved
	}
}
