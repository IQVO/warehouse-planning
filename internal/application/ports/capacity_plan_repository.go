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
	// OutboxRepository in the same UnitOfWork. A save that would leave two
	// PUBLISHED plans with one capacityplan.SupersedeKey (docs/adr/0024
	// decision 3) is ErrVersionConflict and writes nothing: a concurrent
	// publish of the same key won, and the caller reruns its unit.
	Save(ctx context.Context, plan *capacityplan.CapacityPlan) error
	// FindByID returns the plan, or (nil, nil) if none exists.
	FindByID(ctx context.Context, id string) (*capacityplan.CapacityPlan, error)
	// FindPublished returns the PUBLISHED plan with key whose id is not
	// excludeID, or (nil, nil) if there is none. At most one exists (the
	// rule Save enforces); were there several, the newest by published_at,
	// then id, is returned. Inside a UnitOfWork the row is locked like
	// FindByID's, so the plan cannot be superseded twice concurrently.
	FindPublished(ctx context.Context, key capacityplan.SupersedeKey, excludeID string) (*capacityplan.CapacityPlan, error)
}
