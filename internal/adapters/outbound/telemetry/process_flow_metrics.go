package telemetry

import (
	"context"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/metric"

	"github.com/claudioed/warehouse-planning/internal/application/ports"
)

// processFlowMessagesCounterName counts the process-path-management flow
// messages the consumer handled, by outcome (applied, stale, duplicate or
// rejected). <context>.<aggregate>.<verb> (docs/adr/0011).
const processFlowMessagesCounterName = "warehouse_planning.process_flows.messages"

// ProcessFlowMetrics implements ports.ProcessFlowMetrics against the global
// MeterProvider (a no-op until Setup installs a real one).
type ProcessFlowMetrics struct {
	counter metric.Int64Counter
}

// NewProcessFlowMetrics registers the process-flow-messages counter. Like
// NewPlanMetrics it only fails on an invalid instrument name, and its methods
// are nil-safe.
func NewProcessFlowMetrics() (*ProcessFlowMetrics, error) {
	counter, err := otel.Meter(meterName).Int64Counter(
		processFlowMessagesCounterName,
		metric.WithDescription("process-path-management flow messages handled, by outcome (applied, stale, duplicate or rejected)."),
		metric.WithUnit("{message}"),
	)
	if err != nil {
		return nil, err
	}
	return &ProcessFlowMetrics{counter: counter}, nil
}

var _ ports.ProcessFlowMetrics = (*ProcessFlowMetrics)(nil)

// ProcessFlowMessage implements ports.ProcessFlowMetrics. Nil-safe.
func (m *ProcessFlowMetrics) ProcessFlowMessage(ctx context.Context, outcome string) {
	if m == nil {
		return
	}
	m.counter.Add(ctx, 1, metric.WithAttributes(outcomeKey.String(outcome)))
}
