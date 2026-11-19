package ports

import (
	"context"

	"github.com/claudioed/warehouse-planning/internal/domain/labourplan"
)

// LabourPlanRepository persists the LabourPlan aggregate (docs/adr/0023): one
// plan per (site id, shift id, path id). Inside a UnitOfWork the repository
// joins the transaction carried in ctx, so a Save commits or rolls back with
// the outbox insert of its LabourPlanCommitted event. It never persists
// domain events.
type LabourPlanRepository interface {
	// Get returns (nil, false, nil) when no plan exists for (siteID, shiftID, pathID).
	Get(ctx context.Context, siteID, shiftID, pathID string) (*labourplan.Plan, bool, error)
	// Save writes the plan at p.Version() with the same version guard as
	// WorkloadForecastRepository.Save: an INSERT when Version()==1 (a duplicate
	// key is ErrVersionConflict), else an UPDATE guarded by stored version ==
	// Version()-1 (zero rows is ErrVersionConflict).
	Save(ctx context.Context, p *labourplan.Plan) error // same version guard as above
	// List returns the plans of (siteID, shiftID) ordered by path_id, after the cursor's
	// path id (exclusive), at most limit, plus whether more exist.
	List(ctx context.Context, siteID, shiftID, afterPathID string, limit int) ([]*labourplan.Plan, bool, error)
}
