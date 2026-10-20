package memory

import (
	"context"
	"sort"
	"sync"
	"time"

	"github.com/claudioed/warehouse-planning/internal/domain/inbound"
)

// InboundAppointmentRepo is an in-memory, mutex-guarded
// ports.InboundAppointmentRepository. A TEST DOUBLE only (ADR 0016).
type InboundAppointmentRepo struct {
	mu    sync.Mutex
	store map[string]inbound.Appointment
}

// NewInboundAppointmentRepo constructs an empty InboundAppointmentRepo.
func NewInboundAppointmentRepo() *InboundAppointmentRepo {
	return &InboundAppointmentRepo{store: make(map[string]inbound.Appointment)}
}

// Apply implements ports.InboundAppointmentRepository with the same ordering
// guard (inbound.Resolve) the Postgres adapter applies.
func (r *InboundAppointmentRepo) Apply(_ context.Context, next inbound.Appointment) (bool, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	var prev *inbound.Appointment
	if p, ok := r.store[next.ID()]; ok {
		prev = &p
	}
	resolved, changed := inbound.Resolve(prev, next)
	if changed {
		r.store[next.ID()] = resolved
	}
	return changed, nil
}

// Demand implements ports.InboundAppointmentRepository.
func (r *InboundAppointmentRepo) Demand(_ context.Context, location string, start, end time.Time) (inbound.Demand, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	site := make([]inbound.Appointment, 0, len(r.store))
	for _, a := range r.store {
		if a.Location() == location {
			site = append(site, a)
		}
	}
	return inbound.Summarize(site, start, end), nil
}

// Get returns the stored appointment for id -- a test inspection helper.
func (r *InboundAppointmentRepo) Get(id string) (inbound.Appointment, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	a, ok := r.store[id]
	return a, ok
}

// Len is the number of stored appointments -- a test inspection helper.
func (r *InboundAppointmentRepo) Len() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.store)
}

// Snapshot implements Snapshotter.
func (r *InboundAppointmentRepo) Snapshot() func() {
	r.mu.Lock()
	defer r.mu.Unlock()
	saved := make(map[string]inbound.Appointment, len(r.store))
	for k, v := range r.store {
		saved[k] = v
	}
	return func() {
		r.mu.Lock()
		defer r.mu.Unlock()
		r.store = saved
	}
}

type inboundStandardKey struct{ location, processPathID string }

// InboundStandardRepo is an in-memory, mutex-guarded
// ports.InboundStandardRepository. A TEST DOUBLE only (ADR 0016).
type InboundStandardRepo struct {
	mu    sync.Mutex
	store map[inboundStandardKey]inbound.LabourStandard
}

// NewInboundStandardRepo constructs an empty InboundStandardRepo.
func NewInboundStandardRepo() *InboundStandardRepo {
	return &InboundStandardRepo{store: make(map[inboundStandardKey]inbound.LabourStandard)}
}

// Save upserts standard under its (location, process path) key.
func (r *InboundStandardRepo) Save(_ context.Context, standard inbound.LabourStandard) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.store[inboundStandardKey{standard.Location(), standard.ProcessPathID()}] = standard
	return nil
}

// Find returns the standard declared for location + processPathID, or (nil, nil).
func (r *InboundStandardRepo) Find(_ context.Context, location, processPathID string) (*inbound.LabourStandard, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	standard, ok := r.store[inboundStandardKey{location, processPathID}]
	if !ok {
		return nil, nil
	}
	return &standard, nil
}

// List returns the standards of location (every standard when location is
// empty) ordered by location, then process path id.
func (r *InboundStandardRepo) List(_ context.Context, location string) ([]inbound.LabourStandard, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]inbound.LabourStandard, 0, len(r.store))
	for k, standard := range r.store {
		if location == "" || k.location == location {
			out = append(out, standard)
		}
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Location() != out[j].Location() {
			return out[i].Location() < out[j].Location()
		}
		return out[i].ProcessPathID() < out[j].ProcessPathID()
	})
	return out, nil
}
