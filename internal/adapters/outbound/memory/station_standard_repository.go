package memory

import (
	"context"
	"sort"
	"sync"

	"github.com/claudioed/warehouse-planning/internal/domain/processcapacity"
)

type stationStandardKey struct {
	location    string
	processType processcapacity.ProcessType
}

// StationStandardRepo is an in-memory, mutex-guarded
// ports.StationStandardRepository. For tests and local dev only.
type StationStandardRepo struct {
	mu    sync.Mutex
	store map[stationStandardKey]processcapacity.StationStandard
}

// NewStationStandardRepo constructs an empty StationStandardRepo.
func NewStationStandardRepo() *StationStandardRepo {
	return &StationStandardRepo{store: make(map[stationStandardKey]processcapacity.StationStandard)}
}

// Save upserts standard under its (location, process type) key.
func (r *StationStandardRepo) Save(_ context.Context, standard processcapacity.StationStandard) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.store[stationStandardKey{standard.Location(), standard.ProcessType()}] = standard
	return nil
}

// Find returns the standard declared for location + processType, or (nil, nil).
func (r *StationStandardRepo) Find(_ context.Context, location string, processType processcapacity.ProcessType) (*processcapacity.StationStandard, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	standard, ok := r.store[stationStandardKey{location, processType}]
	if !ok {
		return nil, nil
	}
	return &standard, nil
}

// List returns the standards of location (every standard when location is
// empty) ordered by location, then process type.
func (r *StationStandardRepo) List(_ context.Context, location string) ([]processcapacity.StationStandard, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]processcapacity.StationStandard, 0, len(r.store))
	for k, standard := range r.store {
		if location == "" || k.location == location {
			out = append(out, standard)
		}
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Location() != out[j].Location() {
			return out[i].Location() < out[j].Location()
		}
		return out[i].ProcessType() < out[j].ProcessType()
	})
	return out, nil
}
