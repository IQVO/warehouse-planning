package memory

import (
	"context"
	"sync"

	"github.com/claudioed/warehouse-planning/internal/application/ports"
	"github.com/claudioed/warehouse-planning/internal/domain/labourplan"
)

// CommittedHeadsRepo is an in-memory, mutex-guarded
// ports.CommittedHeadsRepository: one commitment per (site id, shift id,
// path id), with the Postgres adapter's upsert and drift-record rules. A TEST
// DOUBLE only (ADR 0016).
type CommittedHeadsRepo struct {
	mu    sync.Mutex
	store map[planKey]labourplan.Commitment
}

// NewCommittedHeadsRepo constructs an empty CommittedHeadsRepo.
func NewCommittedHeadsRepo() *CommittedHeadsRepo {
	return &CommittedHeadsRepo{store: make(map[planKey]labourplan.Commitment)}
}

// Get implements ports.CommittedHeadsRepository.
func (r *CommittedHeadsRepo) Get(_ context.Context, siteID, shiftID, pathID string) (labourplan.Commitment, bool, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	c, ok := r.store[planKey{siteID: siteID, shiftID: shiftID, pathID: pathID}]
	return c, ok, nil
}

// SaveCommitment implements ports.CommittedHeadsRepository: the heads and
// the observation time are replaced, the last recorded drift is kept.
func (r *CommittedHeadsRepo) SaveCommitment(_ context.Context, c labourplan.Commitment) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	key := planKey{siteID: c.SiteID, shiftID: c.ShiftID, pathID: c.PathID}
	stored := r.store[key]
	c.LastDriftHeads, c.HasLastDrift = stored.LastDriftHeads, stored.HasLastDrift
	r.store[key] = c
	return nil
}

// RecordDrift implements ports.CommittedHeadsRepository.
func (r *CommittedHeadsRepo) RecordDrift(_ context.Context, siteID, shiftID, pathID string, driftHeads int) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	key := planKey{siteID: siteID, shiftID: shiftID, pathID: pathID}
	c, ok := r.store[key]
	if !ok {
		return ports.ErrCommittedHeadsNotFound
	}
	c.LastDriftHeads, c.HasLastDrift = driftHeads, true
	r.store[key] = c
	return nil
}

// Snapshot implements Snapshotter.
func (r *CommittedHeadsRepo) Snapshot() func() {
	r.mu.Lock()
	defer r.mu.Unlock()
	saved := make(map[planKey]labourplan.Commitment, len(r.store))
	for k, v := range r.store {
		saved[k] = v
	}
	return func() {
		r.mu.Lock()
		defer r.mu.Unlock()
		r.store = saved
	}
}
