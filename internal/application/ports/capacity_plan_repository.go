package ports

import (
	"context"

	"github.com/claudioed/warehouse-planning/internal/domain/capacityplan"
)

// CapacityPlanRepository persists and retrieves CapacityPlan aggregates by
// id. Inside a UnitOfWork the repository joins the transaction carried in
// ctx; FindByID there also locks the row, so two concurrent Publish calls
// cannot both see a DRAFT plan.
type CapacityPlanRepository interface {
	// Save upserts the plan. It does NOT persist the plan's pending
	// domain events -- the use case pulls those and writes them through
	// OutboxRepository in the same UnitOfWork.
	Save(ctx context.Context, plan *capacityplan.CapacityPlan) error
	// FindByID returns the plan, or (nil, nil) if none exists.
	FindByID(ctx context.Context, id string) (*capacityplan.CapacityPlan, error)
}
