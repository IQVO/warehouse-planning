package usecases

import (
	"context"
	"fmt"

	"github.com/claudioed/warehouse-planning/internal/application/ports"
	"github.com/claudioed/warehouse-planning/internal/domain/capacityplan"
)

// CompareCapacityPlans lays 2 to 5 stored plans of one site and window side
// by side (docs/adr/0024 decision 4). It is a pure read over stored plans:
// nothing is recomputed, so a comparison is reproducible.
type CompareCapacityPlans struct {
	Plans ports.CapacityPlanRepository
}

// Handle returns the plans named by ids, in that order. Errors:
// capacityplan.ErrInvalidComparison (not 2 to 5 distinct, non-blank ids),
// ErrCapacityPlanNotFound for an unknown id (checked before the scope), and
// capacityplan.ErrComparisonSiteMismatch / ErrComparisonWindowMismatch when
// the plans are not all of one site and window.
func (uc *CompareCapacityPlans) Handle(ctx context.Context, ids []string) ([]*capacityplan.CapacityPlan, error) {
	if err := capacityplan.ValidateComparisonIDs(ids); err != nil {
		return nil, err
	}
	plans := make([]*capacityplan.CapacityPlan, 0, len(ids))
	for _, id := range ids {
		plan, err := uc.Plans.FindByID(ctx, id)
		if err != nil {
			return nil, err
		}
		if plan == nil {
			return nil, fmt.Errorf("%w: %s", ErrCapacityPlanNotFound, id)
		}
		plans = append(plans, plan)
	}
	if err := capacityplan.CheckComparable(plans); err != nil {
		return nil, err
	}
	return plans, nil
}
