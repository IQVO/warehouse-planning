package memory

import (
	"context"
	"sync"
	"time"

	"github.com/claudioed/warehouse-planning/internal/domain/observed"
)

type observedKey struct {
	location, processType string
	minute                time.Time
}

type observedBucket struct {
	tasks, durationSeconds, tasksWithDuration int64
	asOf                                      time.Time
}

// ObservedCapacityRepo is an in-memory, mutex-guarded
// ports.ObservedCapacityRepository that keeps the same per-minute buckets the
// Postgres adapter does. A TEST DOUBLE only (ADR 0016).
type ObservedCapacityRepo struct {
	mu      sync.Mutex
	buckets map[observedKey]observedBucket
}

// NewObservedCapacityRepo constructs an empty ObservedCapacityRepo.
func NewObservedCapacityRepo() *ObservedCapacityRepo {
	return &ObservedCapacityRepo{buckets: make(map[observedKey]observedBucket)}
}

// Record implements ports.ObservedCapacityRepository: it adds the completion
// to its (location, process type, minute) bucket.
func (r *ObservedCapacityRepo) Record(_ context.Context, c observed.Completion) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	k := observedKey{c.Location(), c.ProcessType(), c.Minute()}
	b := r.buckets[k]
	b.tasks++
	if d, ok := c.DurationSeconds(); ok {
		b.durationSeconds += d
		b.tasksWithDuration++
	}
	if c.At().After(b.asOf) {
		b.asOf = c.At()
	}
	r.buckets[k] = b
	return nil
}

// Throughput implements ports.ObservedCapacityRepository.
func (r *ObservedCapacityRepo) Throughput(_ context.Context, location, processType string, start, end time.Time) (observed.Throughput, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	var t observed.Throughput
	for k, b := range r.buckets {
		if k.location != location || k.processType != processType {
			continue
		}
		if b.asOf.After(t.AsOf) {
			t.AsOf = b.asOf
		}
		if k.minute.Compare(start) >= 0 && k.minute.Compare(end) < 0 {
			t.Tasks += b.tasks
			t.DurationSeconds += b.durationSeconds
			t.TasksWithDuration += b.tasksWithDuration
		}
	}
	return t, nil
}

// Tasks is the number of recorded completions across every bucket -- a test
// inspection helper.
func (r *ObservedCapacityRepo) Tasks() int64 {
	r.mu.Lock()
	defer r.mu.Unlock()
	var n int64
	for _, b := range r.buckets {
		n += b.tasks
	}
	return n
}

// Snapshot implements Snapshotter.
func (r *ObservedCapacityRepo) Snapshot() func() {
	r.mu.Lock()
	defer r.mu.Unlock()
	saved := make(map[observedKey]observedBucket, len(r.buckets))
	for k, v := range r.buckets {
		saved[k] = v
	}
	return func() {
		r.mu.Lock()
		defer r.mu.Unlock()
		r.buckets = saved
	}
}
