package processcapacity

import (
	"time"
)

// Event names. These are the PascalCase domain event names; the Kafka
// adapter turns them into CloudEvents types
// (com.warehouse.wes.warehouse-planning.processcapacity.<EventName>).
const (
	EventProcessCapacityRegistered = "ProcessCapacityRegistered"
	EventProcessCapacityChanged    = "ProcessCapacityChanged"
)

// Event is a domain event a ProcessCapacity raised. Events are plain
// structs the aggregate accumulates; PullEvents hands them to the
// application layer, which persists them through the transactional outbox.
type Event interface {
	// EventName is the PascalCase domain event name.
	EventName() string
	// AggregateID is the canonical identity string of the ProcessCapacity
	// that raised the event (ProcessType/Location/window, see ID).
	AggregateID() string
	// OccurredAt is when the event happened (domain time, not wall clock
	// at publish time).
	OccurredAt() time.Time
}

// Header is the part every ProcessCapacity event shares: the aggregate's
// full identity (ProcessCapacity has no single id; its identity IS the
// process + location + window triple) and the domain time.
type Header struct {
	ProcessType ProcessType
	Location    string
	WindowStart time.Time
	WindowEnd   time.Time
	At          time.Time
}

// AggregateID implements Event: the same canonical identity string the
// aggregate's own ID method builds, so the CloudEvents subject and the
// Kafka key are stable and match GET /process-capacities lookups.
func (h Header) AggregateID() string {
	return capacityID(h.ProcessType, h.Location, h.WindowStart, h.WindowEnd)
}

// OccurredAt implements Event.
func (h Header) OccurredAt() time.Time { return h.At }

// ProcessCapacityRegistered is raised once, when the FIRST constraint is
// registered on a ProcessCapacity: the constraint that establishes the
// aggregate's native unit and brings it into being. The effective rate at
// that moment IS the registered constraint's rate.
type ProcessCapacityRegistered struct {
	Header
	// ConstraintType is the native constraint that created the aggregate.
	ConstraintType ConstraintType
	// Rate is the native constraint's rate (and the effective rate).
	Rate CapacityRate
}

// EventName implements Event.
func (ProcessCapacityRegistered) EventName() string { return EventProcessCapacityRegistered }

// ProcessCapacityChanged is raised when a registration changes the
// aggregate's effective rate: the minimum rate value, the binding
// constraint, or both. A registration that leaves the effective rate
// untouched (a non-binding constraint added, or a constraint re-registered
// with the rate it already had) raises nothing.
type ProcessCapacityChanged struct {
	Header
	// ConstraintType is the constraint whose registration caused the
	// change (a fresh one or a re-registered one).
	ConstraintType ConstraintType
	// PreviousRate and PreviousBinding are the effective rate before the
	// registration.
	PreviousRate    CapacityRate
	PreviousBinding ConstraintType
	// NewRate and NewBinding are the effective rate after it.
	NewRate    CapacityRate
	NewBinding ConstraintType
}

// EventName implements Event.
func (ProcessCapacityChanged) EventName() string { return EventProcessCapacityChanged }
