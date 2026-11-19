package ports

import (
	"github.com/claudioed/warehouse-planning/internal/application/outbox"
	"github.com/claudioed/warehouse-planning/internal/domain/capacityplan"
	"github.com/claudioed/warehouse-planning/internal/domain/labourplan"
	"github.com/claudioed/warehouse-planning/internal/domain/processcapacity"
	"github.com/claudioed/warehouse-planning/internal/domain/workloadforecast"
)

// EventEncoder turns domain events into wire-ready outbox messages
// (CloudEvents 1.0 structured mode, each with a freshly minted id). It is
// pure -- no I/O -- so the use cases can call it inside their
// UnitOfWork. The Kafka adapter implements it through
// internal/adapters/kafka/cloudevents, the only place envelopes are built.
//
// One method per event-bearing aggregate (or per event, when the aggregate
// raises unrelated facts) keeps the event kinds sealed: only these domain
// event values can be encoded, and an implementation (the analytics fanout)
// can treat the kinds differently without sniffing concrete types.
type EventEncoder interface {
	// EncodeCapacityPlanEvents encodes CapacityPlan events onto the
	// integration topic (the analytics fanout also enqueues the analytics
	// copy of each).
	EncodeCapacityPlanEvents(events ...capacityplan.Event) ([]outbox.Message, error)
	// EncodeProcessCapacityEvents encodes ProcessCapacity events onto the
	// integration topic (no analytics copy: the analytics stream is plan
	// facts only, ADR 0005).
	EncodeProcessCapacityEvents(events ...processcapacity.Event) ([]outbox.Message, error)
	// EncodeWorkloadForecastEvents encodes WorkloadForecastReceived events
	// (ADR 0023 decision 1) onto the integration topic; the analytics
	// fanout also enqueues the analytics copy of each, under the same id.
	EncodeWorkloadForecastEvents(events ...workloadforecast.Received) ([]outbox.Message, error)
	// EncodeLabourPlanCommitted encodes LabourPlanCommitted events (ADR 0023
	// decision 2) onto the integration topic; the analytics fanout also
	// enqueues the analytics copy of each, under the same id.
	EncodeLabourPlanCommitted(events ...labourplan.Committed) ([]outbox.Message, error)
	// EncodeLabourPlanDriftDetected encodes LabourPlanDriftDetected events
	// (ADR 0023 decision 3) onto the integration topic; the analytics fanout
	// also enqueues the analytics copy of each, under the same id.
	EncodeLabourPlanDriftDetected(events ...labourplan.DriftDetected) ([]outbox.Message, error)
}
