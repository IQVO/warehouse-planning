package usecases

import (
	"context"

	"github.com/claudioed/warehouse-planning/internal/application/ports"
	"github.com/claudioed/warehouse-planning/internal/domain/capacityplan"
)

const (
	// DefaultCapacityPlanListLimit is how many plans ListCapacityPlans
	// returns when the caller states no limit.
	DefaultCapacityPlanListLimit = 20
	// MaxCapacityPlanListLimit caps a stated limit: a larger request is
	// served with this many plans rather than rejected.
	MaxCapacityPlanListLimit = 100
)

// ListCapacityPlans returns the most recently created CapacityPlans,
// optionally for one location. It is a pure read: it neither mutates a plan
// nor queues an event.
type ListCapacityPlans struct {
	Plans ports.CapacityPlanLister
}

// Handle lists up to limit plans, newest first. A limit below 1 means "the
// default" (DefaultCapacityPlanListLimit); one above MaxCapacityPlanListLimit
// is capped to it. An empty location means every location.
func (uc *ListCapacityPlans) Handle(ctx context.Context, location string, limit int) ([]*capacityplan.CapacityPlan, error) {
	if limit < 1 {
		limit = DefaultCapacityPlanListLimit
	}
	if limit > MaxCapacityPlanListLimit {
		limit = MaxCapacityPlanListLimit
	}
	plans, err := uc.Plans.ListRecent(ctx, location, limit)
	if err != nil {
		return nil, err
	}
	if plans == nil {
		plans = []*capacityplan.CapacityPlan{}
	}
	return plans, nil
}
