package ports

import (
	"context"

	"github.com/claudioed/warehouse-planning/internal/domain/capacityplan"
)

// CapacityPlanLister is the read-only list side of the CapacityPlan store. It
// is a separate port from CapacityPlanRepository (which every write use case
// and the MCP tools depend on) so the list read does not widen that
// interface; the memory and Postgres repositories implement both.
type CapacityPlanLister interface {
	// ListRecent returns at most limit plans, most recently created first
	// (ties broken by id, descending, so the order is total). A non-empty
	// location restricts the result to that location; empty means every
	// location. The caller supplies a positive limit (the use case applies
	// the default and the cap). It returns an empty, non-nil slice when
	// nothing matches and never locks a row.
	ListRecent(ctx context.Context, location string, limit int) ([]*capacityplan.CapacityPlan, error)
}
