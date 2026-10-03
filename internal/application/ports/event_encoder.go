package ports

import (
	"github.com/claudioed/warehouse-planning/internal/application/outbox"
	"github.com/claudioed/warehouse-planning/internal/domain/capacityplan"
)

// EventEncoder turns CapacityPlan domain events into wire-ready outbox
// messages (CloudEvents 1.0 structured mode, each with a freshly minted
// id). It is pure -- no I/O -- so the use case can call it inside its
// UnitOfWork. The Kafka adapter implements it through
// internal/adapters/kafka/cloudevents, the only place envelopes are built.
type EventEncoder interface {
	Encode(events ...capacityplan.Event) ([]outbox.Message, error)
}
