package memory

import (
	"context"
	"sort"
	"sync"

	"github.com/claudioed/warehouse-planning/internal/domain/labourplan"
)

// LabourPlanRepo is an in-memory, mutex-guarded ports.LabourPlanRepository:
// one plan per (site id, shift id, path id), with the Postgres adapter's
// version guard and list order (path id, byte order). A TEST DOUBLE only
// (ADR 0016).
type LabourPlanRepo struct {
	mu    sync.Mutex
	store map[planKey]storedPlan
}

type planKey struct{ siteID, shiftID, pathID string }

// storedPlan is a row: the spec is a copy no caller holds.
type storedPlan struct {
	spec    labourplan.Spec
	version int64
}

// NewLabourPlanRepo constructs an empty LabourPlanRepo.
func NewLabourPlanRepo() *LabourPlanRepo {
	return &LabourPlanRepo{store: make(map[planKey]storedPlan)}
}

// Get implements ports.LabourPlanRepository. It rehydrates a fresh plan
// through labourplan.Rehydrate, so a caller changing it never touches the
// stored row.
func (r *LabourPlanRepo) Get(_ context.Context, siteID, shiftID, pathID string) (*labourplan.Plan, bool, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	s, ok := r.store[planKey{siteID: siteID, shiftID: shiftID, pathID: pathID}]
	if !ok {
		return nil, false, nil
	}
	p, err := labourplan.Rehydrate(s.spec, s.version)
	if err != nil {
		return nil, false, err
	}
	return p, true, nil
}

// Save implements ports.LabourPlanRepository with the same version guard as
// the Postgres adapter (versionGuard).
func (r *LabourPlanRepo) Save(_ context.Context, p *labourplan.Plan) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	spec := p.Spec()
	key := planKey{siteID: spec.SiteID, shiftID: spec.ShiftID, pathID: spec.PathID}
	stored, exists := r.store[key]
	if err := versionGuard(exists, stored.version, p.Version()); err != nil {
		return err
	}
	r.store[key] = storedPlan{spec: spec, version: p.Version()}
	return nil
}

// List implements ports.LabourPlanRepository: the plans of (siteID,
// shiftID) whose path id sorts after afterPathID (byte order, like the
// Postgres column's "C" collation), at most limit (a negative limit is 0),
// and whether more follow. It returns an empty, non-nil slice when nothing
// matches.
func (r *LabourPlanRepo) List(_ context.Context, siteID, shiftID, afterPathID string, limit int) ([]*labourplan.Plan, bool, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	limit = max(limit, 0)
	matching := make([]storedPlan, 0, len(r.store))
	for k, s := range r.store {
		if k.siteID == siteID && k.shiftID == shiftID && k.pathID > afterPathID {
			matching = append(matching, s)
		}
	}
	sort.Slice(matching, func(i, j int) bool { return matching[i].spec.PathID < matching[j].spec.PathID })
	more := len(matching) > limit
	if more {
		matching = matching[:limit]
	}
	out := make([]*labourplan.Plan, 0, len(matching))
	for _, s := range matching {
		p, err := labourplan.Rehydrate(s.spec, s.version)
		if err != nil {
			return nil, false, err
		}
		out = append(out, p)
	}
	return out, more, nil
}

// Snapshot implements Snapshotter.
func (r *LabourPlanRepo) Snapshot() func() {
	r.mu.Lock()
	defer r.mu.Unlock()
	saved := make(map[planKey]storedPlan, len(r.store))
	for k, v := range r.store {
		saved[k] = v
	}
	return func() {
		r.mu.Lock()
		defer r.mu.Unlock()
		r.store = saved
	}
}
