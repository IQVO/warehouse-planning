package telemetry

import (
	"context"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"

	"github.com/claudioed/warehouse-planning/internal/application/ports"
)

// meterName scopes this service's own instruments, keeping them distinct
// from the ones otelchi and the runtime collector register.
const meterName = "github.com/claudioed/warehouse-planning"

// capacityPlanCreatedCounterName is the Tier-2 business metric (fleet
// standard-metrics ADR, docs/adr/0011): how CreateCapacityPlan outcomes
// split between actually created and rejected, by outcome attribute
// rather than two separately-named counters. Matches the ADR's naming
// convention (<context>.<aggregate>.<verb>) exactly.
const capacityPlanCreatedCounterName = "warehouse_planning.capacity_plans.created"

// outcomeKey distinguishes created from rejected attempts on the single
// counter.
const outcomeKey = attribute.Key("outcome")

// PlanMetrics implements ports.PlanMetrics against the global
// MeterProvider. Until Setup installs a real provider, the global one is a
// no-op, so recording is cheap and safe in tests and local runs.
type PlanMetrics struct {
	counter metric.Int64Counter
}

// NewPlanMetrics registers the capacity-plans-created counter. It only
// fails if the instrument name is invalid, which is a programming error,
// not a runtime condition -- callers that would rather run
// un-instrumented than not at all can ignore the error and pass a nil
// *PlanMetrics instead (its methods are nil-safe, see below).
func NewPlanMetrics() (*PlanMetrics, error) {
	counter, err := otel.Meter(meterName).Int64Counter(
		capacityPlanCreatedCounterName,
		metric.WithDescription("CreateCapacityPlan attempts, by outcome (created or rejected)."),
		metric.WithUnit("{plan}"),
	)
	if err != nil {
		return nil, err
	}
	return &PlanMetrics{counter: counter}, nil
}

var _ ports.PlanMetrics = (*PlanMetrics)(nil)

// CapacityPlanCreated implements ports.PlanMetrics. Nil-safe: a nil
// *PlanMetrics receiver is a documented no-op, matching
// process-path-management's PathMetrics nil-safety convention.
func (m *PlanMetrics) CapacityPlanCreated(ctx context.Context, outcome string) {
	if m == nil {
		return
	}
	m.counter.Add(ctx, 1, metric.WithAttributes(outcomeKey.String(outcome)))
}
