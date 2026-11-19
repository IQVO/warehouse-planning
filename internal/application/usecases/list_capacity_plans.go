package usecases

import (
	"context"

	"github.com/claudioed/warehouse-planning/internal/domain/pagecursor"

	"github.com/claudioed/warehouse-planning/internal/application/ports"
	"github.com/claudioed/warehouse-planning/internal/domain/capacityplan"
)

// ListCapacityPlans returns the most recently created CapacityPlans matching
// a filter (location, site, path, window: every one that is set,
// docs/adr/0024), one keyset page at a time. It is a pure read: it neither
// mutates a plan nor queues an event.
type ListCapacityPlans struct {
	Plans ports.CapacityPlanLister
}

// Handle lists up to limit plans matching filter, newest first, strictly
// after the cursor (nil = first page). The zero filter lists every plan. The
// caller owns the limit bounds (the inbound adapter's page package).
func (uc *ListCapacityPlans) Handle(ctx context.Context, filter capacityplan.ListFilter, after *pagecursor.Cursor, limit int) ([]*capacityplan.CapacityPlan, error) {
	plans, err := uc.Plans.ListRecent(ctx, filter, after, limit)
	if err != nil {
		return nil, err
	}
	if plans == nil {
		plans = []*capacityplan.CapacityPlan{}
	}
	return plans, nil
}
