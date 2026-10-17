package ports

import (
	"github.com/claudioed/warehouse-planning/internal/application/outbox"
	"github.com/claudioed/warehouse-planning/internal/domain/capacityplan"
	"github.com/claudioed/warehouse-planning/internal/domain/processcapacity"
)

// EventEncoder turns domain events into wire-ready outbox messages
// (CloudEvents 1.0 structured mode, each with a freshly minted id). It is
// pure -- no I/O -- so the use cases can call it inside their
// UnitOfWork. The Kafka adapter implements it through
// internal/adapters/kafka/cloudevents, the only place envelopes are built.
//
// One method per event-bearing aggregate keeps the event kinds sealed:
// only capacityplan.Event and processcapacity.Event values can be encoded,
// and an implementation (the analytics fanout) can treat the two kinds
// differently without sniffing concrete types.
type EventEncoder interface {
	// EncodeCapacityPlanEvents encodes CapacityPlan events onto the
	// integration topic (the analytics fanout also enqueues the analytics
	// copy of each).
	EncodeCapacityPlanEvents(events ...capacityplan.Event) ([]outbox.Message, error)
	// EncodeProcessCapacityEvents encodes ProcessCapacity events onto the
	// integration topic (no analytics copy: the analytics stream is plan
	// facts only, ADR 0005).
	EncodeProcessCapacityEvents(events ...processcapacity.Event) ([]outbox.Message, error)
}
