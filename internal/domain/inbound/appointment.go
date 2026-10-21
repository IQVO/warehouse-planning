// Package inbound holds the inbound labour demand rules of the planning
// context (docs/adr/0017): the dock-appointment read model fed by
// inbound-receiving's DockAppointmentBooked / DockAppointmentCancelled
// events, the ordering guard that makes the two events order-independent,
// and the operator-declared labour standard that turns appointments into
// labour minutes.
//
// The package performs NO I/O and imports nothing from this module: it is
// pure rules over values handed in by the Kafka consumer and the use cases.
package inbound

import (
	"errors"
	"time"
)

var (
	// ErrRequiredField is returned when the appointment id or the site is
	// blank.
	ErrRequiredField = errors.New("inbound: appointment id and location are required")

	// ErrAsOfRequired is returned when the producer's event time is missing:
	// it is the ordering key, so an event without it cannot be ordered
	// against another one.
	ErrAsOfRequired = errors.New("inbound: the event time (as-of) is required")

	// ErrInvalidWindow is returned when a booking's door window does not end
	// strictly after it starts.
	ErrInvalidWindow = errors.New("inbound: the appointment window must end after it starts")

	// ErrASNRequired is returned when a booking covers no ASN (the producer
	// guarantees at least one).
	ErrASNRequired = errors.New("inbound: a booked appointment covers at least one ASN")
)

// Status is the lifecycle state the read model keeps for an appointment.
type Status string

// The two states of the read model. Only BOOKED appointments are demand.
const (
	StatusBooked    Status = "BOOKED"
	StatusCancelled Status = "CANCELLED"
)

// Appointment is one dock appointment as the inbound demand read model knows
// it: where its demand is attributed, the door window, how many ASNs it
// covers, its state and the time of the producer event that last wrote it.
//
// A CANCELLED appointment whose booking was never seen is a tombstone: it has
// no window and no ASN count, and only exists to make a later (older)
// DockAppointmentBooked stale.
type Appointment struct {
	id          string
	location    string
	status      Status
	windowStart time.Time
	windowEnd   time.Time
	asnCount    int
	asOf        time.Time
}

// BookedParams carries everything NewBooked needs.
type BookedParams struct {
	AppointmentID string
	Location      string
	WindowStart   time.Time
	WindowEnd     time.Time
	// ASNCount is len(data.asn_numbers).
	ASNCount int
	// AsOf is the CloudEvents `time` of the DockAppointmentBooked event.
	AsOf time.Time
}

// NewBooked validates p and builds a BOOKED Appointment. Instants are
// normalized to UTC.
func NewBooked(p BookedParams) (Appointment, error) {
	if p.AppointmentID == "" || p.Location == "" {
		return Appointment{}, ErrRequiredField
	}
	if p.AsOf.IsZero() {
		return Appointment{}, ErrAsOfRequired
	}
	if p.WindowStart.IsZero() || !p.WindowEnd.After(p.WindowStart) {
		return Appointment{}, ErrInvalidWindow
	}
	if p.ASNCount < 1 {
		return Appointment{}, ErrASNRequired
	}
	return Appointment{
		id:          p.AppointmentID,
		location:    p.Location,
		status:      StatusBooked,
		windowStart: p.WindowStart.UTC(),
		windowEnd:   p.WindowEnd.UTC(),
		asnCount:    p.ASNCount,
		asOf:        p.AsOf.UTC(),
	}, nil
}

// NewCancelled validates its arguments and builds a CANCELLED Appointment
// carrying no window (a tombstone until merged with a booking by Resolve).
func NewCancelled(appointmentID, location string, asOf time.Time) (Appointment, error) {
	if appointmentID == "" || location == "" {
		return Appointment{}, ErrRequiredField
	}
	if asOf.IsZero() {
		return Appointment{}, ErrAsOfRequired
	}
	return Appointment{id: appointmentID, location: location, status: StatusCancelled, asOf: asOf.UTC()}, nil
}

// RehydrateParams is the stored state of an appointment, used by repository
// adapters to rebuild it without re-validating.
type RehydrateParams struct {
	AppointmentID string
	Location      string
	Status        Status
	// WindowStart, WindowEnd and ASNCount are the zero values for a
	// tombstone.
	WindowStart time.Time
	WindowEnd   time.Time
	ASNCount    int
	AsOf        time.Time
}

// Rehydrate rebuilds an Appointment from persisted state. It is for
// repository adapters only; the consumer goes through NewBooked /
// NewCancelled.
func Rehydrate(p RehydrateParams) Appointment {
	a := Appointment{
		id: p.AppointmentID, location: p.Location, status: p.Status,
		asnCount: p.ASNCount, asOf: p.AsOf.UTC(),
	}
	if !p.WindowStart.IsZero() {
		a.windowStart, a.windowEnd = p.WindowStart.UTC(), p.WindowEnd.UTC()
	}
	return a
}

// ID returns the inbound-receiving appointment id.
func (a Appointment) ID() string { return a.id }

// Location returns the site the appointment's demand is attributed to.
func (a Appointment) Location() string { return a.location }

// Status returns BOOKED or CANCELLED.
func (a Appointment) Status() Status { return a.status }

// WindowStart returns the start of the door window (UTC); the zero time for
// a tombstone.
func (a Appointment) WindowStart() time.Time { return a.windowStart }

// WindowEnd returns the end of the door window (UTC); the zero time for a
// tombstone.
func (a Appointment) WindowEnd() time.Time { return a.windowEnd }

// ASNCount returns how many ASNs the appointment covers; 0 for a tombstone.
func (a Appointment) ASNCount() int { return a.asnCount }

// AsOf returns the time of the producer event that last wrote the
// appointment (UTC).
func (a Appointment) AsOf() time.Time { return a.asOf }

// HasWindow reports whether the booking's window is known (false for a
// tombstone).
func (a Appointment) HasWindow() bool { return !a.windowStart.IsZero() }

// CountsIn reports whether the appointment is demand in the half-open window
// [start, end): it is BOOKED and its door window STARTS at or after start and
// strictly before end. A start exactly at `start` counts, exactly at `end`
// belongs to the next window, so adjacent windows never count an appointment
// twice (docs/adr/0003, docs/adr/0017 decision 4).
func (a Appointment) CountsIn(start, end time.Time) bool {
	return a.status == StatusBooked && a.HasWindow() &&
		a.windowStart.Compare(start) >= 0 && a.windowStart.Compare(end) < 0
}

// Resolve decides what the read model stores when next arrives and prev (nil
// when the id is new) is already stored. It is the ordering guard of
// docs/adr/0017 decision 5 and makes DockAppointmentBooked and
// DockAppointmentCancelled order-independent: last writer wins on the
// producer's event time, and on an equal time Cancelled wins.
//
//   - nothing stored: next is stored (a Cancelled that arrives first is a
//     tombstone);
//   - next is newer: next wins; a cancellation keeps the booking's window and
//     ASN count already known;
//   - next is older: stale, nothing changes (a Booked that arrives after the
//     Cancelled that supersedes it is ignored);
//   - same time: a Cancelled replaces a Booked, never the reverse.
//
// The second result reports whether the model changed.
func Resolve(prev *Appointment, next Appointment) (Appointment, bool) {
	if prev == nil {
		return next, true
	}
	switch c := next.asOf.Compare(prev.asOf); {
	case c < 0:
		return *prev, false
	case c == 0 && (next.status != StatusCancelled || prev.status != StatusBooked):
		return *prev, false
	}
	return merge(*prev, next), true
}

// merge returns next, except that a cancellation inherits the window and ASN
// count of the stored booking so the row still describes what was cancelled.
func merge(prev, next Appointment) Appointment {
	if next.status == StatusCancelled && !next.HasWindow() && prev.HasWindow() {
		next.windowStart, next.windowEnd, next.asnCount = prev.windowStart, prev.windowEnd, prev.asnCount
	}
	return next
}

// Demand is the inbound demand of one site over one window: how many BOOKED
// appointments start in it and how many ASNs they cover.
type Demand struct {
	Appointments int
	ASNs         int
	// AsOf is the newest event time the site's model reflects (any
	// appointment, in or out of the window); the zero time when the site has
	// no appointment.
	AsOf time.Time
}

// Summarize counts the appointments of ONE site that are demand in
// [start, end). It is the reference implementation the in-memory repository
// uses; the Postgres adapter computes the same figures in SQL and is checked
// against the same boundary cases.
func Summarize(appointments []Appointment, start, end time.Time) Demand {
	var d Demand
	for _, a := range appointments {
		if a.asOf.After(d.AsOf) {
			d.AsOf = a.asOf
		}
		if a.CountsIn(start, end) {
			d.Appointments++
			d.ASNs += a.asnCount
		}
	}
	return d
}
