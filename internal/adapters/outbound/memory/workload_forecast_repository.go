package memory

import (
	"context"
	"sync"
	"time"

	"github.com/claudioed/warehouse-planning/internal/application/ports"
	"github.com/claudioed/warehouse-planning/internal/domain/workloadforecast"
)

// WorkloadForecastRepo is an in-memory, mutex-guarded
// ports.WorkloadForecastRepository: one forecast per (site id, path id), with
// the Postgres adapter's version guard. A TEST DOUBLE only (ADR 0016).
type WorkloadForecastRepo struct {
	mu    sync.Mutex
	store map[forecastKey]storedForecast
}

type forecastKey struct{ siteID, pathID string }

// storedForecast is a row: the buckets are a copy no caller holds.
type storedForecast struct {
	buckets    []workloadforecast.Bucket
	receivedAt time.Time
	version    int64
}

// NewWorkloadForecastRepo constructs an empty WorkloadForecastRepo.
func NewWorkloadForecastRepo() *WorkloadForecastRepo {
	return &WorkloadForecastRepo{store: make(map[forecastKey]storedForecast)}
}

// Get implements ports.WorkloadForecastRepository. It rehydrates a fresh
// forecast through workloadforecast.Rehydrate, so a caller changing it never
// touches the stored row.
func (r *WorkloadForecastRepo) Get(_ context.Context, siteID, pathID string) (*workloadforecast.Forecast, bool, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	s, ok := r.store[forecastKey{siteID: siteID, pathID: pathID}]
	if !ok {
		return nil, false, nil
	}
	f, err := workloadforecast.Rehydrate(siteID, pathID, s.buckets, s.receivedAt, s.version)
	if err != nil {
		return nil, false, err
	}
	return f, true, nil
}

// Save implements ports.WorkloadForecastRepository with the same version
// guard as the Postgres adapter (versionGuard).
func (r *WorkloadForecastRepo) Save(_ context.Context, f *workloadforecast.Forecast) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	key := forecastKey{siteID: f.SiteID(), pathID: f.PathID()}
	stored, exists := r.store[key]
	if err := versionGuard(exists, stored.version, f.Version()); err != nil {
		return err
	}
	r.store[key] = storedForecast{buckets: f.Buckets(), receivedAt: f.ReceivedAt(), version: f.Version()}
	return nil
}

// Snapshot implements Snapshotter.
func (r *WorkloadForecastRepo) Snapshot() func() {
	r.mu.Lock()
	defer r.mu.Unlock()
	saved := make(map[forecastKey]storedForecast, len(r.store))
	for k, v := range r.store {
		saved[k] = v
	}
	return func() {
		r.mu.Lock()
		defer r.mu.Unlock()
		r.store = saved
	}
}

// versionGuard is the optimistic-concurrency rule of the versioned
// repositories (ports.WorkloadForecastRepository, ports.LabourPlanRepository):
// version 1 is a first save and needs no stored row; any later version needs
// the stored row at exactly version-1. Anything else is
// ports.ErrVersionConflict.
func versionGuard(exists bool, stored, next int64) error {
	if next == 1 {
		if exists {
			return ports.ErrVersionConflict
		}
		return nil
	}
	if !exists || stored != next-1 {
		return ports.ErrVersionConflict
	}
	return nil
}
