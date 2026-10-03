package memory

import (
	"context"
	"sync"
)

// ProcessedEventRepo is an in-memory, mutex-guarded
// ports.ProcessedEventRepository. For tests only.
type ProcessedEventRepo struct {
	mu   sync.Mutex
	seen map[string]struct{}
}

// NewProcessedEventRepo constructs an empty ProcessedEventRepo.
func NewProcessedEventRepo() *ProcessedEventRepo {
	return &ProcessedEventRepo{seen: make(map[string]struct{})}
}

func processedEventKey(consumer, eventID string) string {
	return consumer + "\x00" + eventID
}

// Claim records (consumer, eventID) as processed, returning false if it
// was already recorded by an earlier call.
func (r *ProcessedEventRepo) Claim(_ context.Context, consumer, eventID string) (bool, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	key := processedEventKey(consumer, eventID)
	if _, exists := r.seen[key]; exists {
		return false, nil
	}
	r.seen[key] = struct{}{}
	return true, nil
}

// Snapshot implements Snapshotter.
func (r *ProcessedEventRepo) Snapshot() func() {
	r.mu.Lock()
	defer r.mu.Unlock()
	saved := make(map[string]struct{}, len(r.seen))
	for k := range r.seen {
		saved[k] = struct{}{}
	}
	return func() {
		r.mu.Lock()
		defer r.mu.Unlock()
		r.seen = saved
	}
}

// Has reports whether (consumer, eventID) is currently recorded as
// processed -- a test inspection helper.
func (r *ProcessedEventRepo) Has(consumer, eventID string) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	_, ok := r.seen[processedEventKey(consumer, eventID)]
	return ok
}
