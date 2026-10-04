package capacityplan

import (
	"time"

	"github.com/claudioed/warehouse-planning/internal/domain/processcapacity"
)

// Event names. These are the PascalCase domain event names; the Kafka
// adapter turns them into CloudEvents types
// (com.warehouse.wes.warehouse-planning.capacityplan.<EventName>).
const (
	EventCapacityPlanCreated      = "CapacityPlanCreated"
	EventCapacityPlanPublished    = "CapacityPlanPublished"
	EventCapacityShortageDetected = "CapacityShortageDetected"
	EventBottleneckDetected       = "BottleneckDetected"
)

// Event is a domain event a CapacityPlan raised. Events are plain structs
// the aggregate accumulates; PullEvents hands them to the application
// layer, which persists them through the transactional outbox.
type Event interface {
	// EventName is the PascalCase domain event name.
	EventName() string
	// AggregateID is the id of the CapacityPlan that raised the event.
	AggregateID() string
	// OccurredAt is when the event happened (domain time, not wall clock
	// at publish time).
	OccurredAt() time.Time
}

// Header is the part every CapacityPlan event shares.
type Header struct {
	PlanID string
	At     time.Time
}

// AggregateID implements Event.
func (h Header) AggregateID() string { return h.PlanID }

// OccurredAt implements Event.
func (h Header) OccurredAt() time.Time { return h.At }

// CapacityPlanCreated is raised once, when a plan is created (DRAFT).
type CapacityPlanCreated struct {
	Header
	WarehouseID        string
	Location           string
	PathID             string
	WindowStart        time.Time
	WindowEnd          time.Time
	AssignedDemand     float64
	PathCapacity       float64
	CapacityOverWindow float64
	Shortage           float64
	BottleneckStep     processcapacity.ProcessType
}

// EventName implements Event.
func (CapacityPlanCreated) EventName() string { return EventCapacityPlanCreated }

// CapacityPlanPublished is raised when a DRAFT plan is published.
type CapacityPlanPublished struct {
	Header
	WarehouseID        string
	Location           string
	PathID             string
	WindowStart        time.Time
	WindowEnd          time.Time
	AssignedDemand     float64
	PathCapacity       float64
	CapacityOverWindow float64
	Shortage           float64
	BottleneckStep     processcapacity.ProcessType

	// BottleneckConstraint is the constraint type binding the bottleneck
	// step at creation (LABOR, STATION, ...); empty for a plan created before
	// it was recorded. The integration payload does NOT carry it: only the
	// analytics stream does (bottleneck-frequency report, ADR 0005).
	BottleneckConstraint processcapacity.ConstraintType
}

// EventName implements Event.
func (CapacityPlanPublished) EventName() string { return EventCapacityPlanPublished }

// CapacityShortageDetected is raised at publish time, only when the
// plan's Shortage > 0.
type CapacityShortageDetected struct {
	Header
	WarehouseID        string
	Location           string
	PathID             string
	WindowStart        time.Time
	WindowEnd          time.Time
	AssignedDemand     float64
	CapacityOverWindow float64
	Shortage           float64
	BottleneckStep     processcapacity.ProcessType
}

// EventName implements Event.
func (CapacityShortageDetected) EventName() string { return EventCapacityShortageDetected }

// BottleneckDetected names the path step limiting end-to-end flow. It is
// raised at publish time, only when the plan has a shortage (a bottleneck
// is only actionable when demand cannot be served).
type BottleneckDetected struct {
	Header
	WarehouseID    string
	Location       string
	PathID         string
	WindowStart    time.Time
	WindowEnd      time.Time
	BottleneckStep processcapacity.ProcessType
	PathCapacity   float64
}

// EventName implements Event.
func (BottleneckDetected) EventName() string { return EventBottleneckDetected }
