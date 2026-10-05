package ports

import "context"

// PlanMetrics is the Tier-2 business-metrics port for CapacityPlan
// creation (fleet standard-metrics ADR): a single counter,
// warehouse_planning.capacity_plans.created, with an outcome attribute,
// rather than two separately-named counters for the same event's
// success/failure split. Nil-safe implementations (see
// internal/adapters/outbound/telemetry.PlanMetrics) let a caller that
// predates this port omit it with no behaviour change.
type PlanMetrics interface {
	// CapacityPlanCreated records one CreateCapacityPlan outcome:
	// "created" for a successful Handle, "rejected" for any error
	// returned to the caller (window/path/demand validation failures).
	CapacityPlanCreated(ctx context.Context, outcome string)
}

// Outcome values recorded by PlanMetrics.CapacityPlanCreated.
const (
	PlanOutcomeCreated  = "created"
	PlanOutcomeRejected = "rejected"
)
