package usecases

import (
	"context"

	"github.com/claudioed/warehouse-planning/internal/domain/pagecursor"

	"github.com/claudioed/warehouse-planning/internal/application/ports"
	"github.com/claudioed/warehouse-planning/internal/domain/capacityplan"
)

// ListCapacityPlans returns the most recently created CapacityPlans,
// optionally for one location, one keyset page at a time. It is a pure read:
// it neither mutates a plan nor queues an event.
type ListCapacityPlans struct {
	Plans ports.CapacityPlanLister
}

// Handle lists up to limit plans, newest first, strictly after the cursor
// (nil = first page). An empty location means every location. The caller
// owns the limit bounds (the inbound adapter's page package).
func (uc *ListCapacityPlans) Handle(ctx context.Context, location string, after *pagecursor.Cursor, limit int) ([]*capacityplan.CapacityPlan, error) {
	plans, err := uc.Plans.ListRecent(ctx, location, after, limit)
	if err != nil {
		return nil, err
	}
	if plans == nil {
		plans = []*capacityplan.CapacityPlan{}
	}
	return plans, nil
}
