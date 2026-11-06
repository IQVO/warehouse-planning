// Package inbound holds the inbound labour demand rules of the planning
// context (docs/adr/0017, source switched by docs/adr/0020): the
// dock-appointment read model fed by dock-scheduling's DockAppointmentBooked /
// Rescheduled / Cancelled / NoShow events, the version guard that makes those
// events order-independent, and the operator-declared labour standard that
// turns appointments into labour minutes.
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
	ErrRequiredField = errors.New("inbound: appointment id and site are required")

	// ErrVersionRequired is returned when the producer's aggregate version is
	// missing (< 1): it is the ordering key, so an event without it cannot be
	// ordered against another one.
	ErrVersionRequired = errors.New("inbound: the aggregate version (>= 1) is required")

	// ErrAsOfRequired is returned when the producer's event time is missing.
	// It no longer orders anything (the version does); it stays the
	// "newest event time the model reflects" shown by the read side.
	ErrAsOfRequired = errors.New("inbound: the event time (as-of) is required")

	// ErrInvalidWindow is returned when a door window does not end strictly
	// after it starts.
	ErrInvalidWindow = errors.New("inbound: the appointment window must end after it starts")

	// ErrASNRequired is returned when an INBOUND booking covers no ASN (the
	// producer guarantees at least one).
	ErrASNRequired = errors.New("inbound: a booked INBOUND appointment covers at least one ASN")

	// ErrInvalidDirection is returned when a direction is neither INBOUND nor
	// OUTBOUND.
	ErrInvalidDirection = errors.New("inbound: direction must be INBOUND or OUTBOUND")
)

// Status is the lifecycle state the read model keeps for an appointment.
type Status string

// The states of the read model. Only BOOKED appointments are demand;
// CANCELLED and NO_SHOW both REMOVE the demand (docs/adr/0020).
const (
	StatusBooked    Status = "BOOKED"
	StatusCancelled Status = "CANCELLED"
	StatusNoShow    Status = "NO_SHOW"
)

// Direction is the flow of an appointment. Only INBOUND appointments are
// inbound labour demand; OUTBOUND ones are stored so that a later event of the
// same id can be told apart, and never counted.
type Direction string

// The two directions dock-scheduling publishes.
const (
	DirectionInbound  Direction = "INBOUND"
	DirectionOutbound Direction = "OUTBOUND"
)

// Valid reports whether d is INBOUND or OUTBOUND.
func (d Direction) Valid() bool { return d == DirectionInbound || d == DirectionOutbound }

// Appointment is one dock appointment as the inbound demand read model knows
// it: the site its demand is attributed to (the event's site_id), its
// direction, the door window, how many ASNs it covers, its state, the
// aggregate version of the last accepted state change and the time of the
// producer event that wrote it.
//
// Dock-scheduling's events are not all complete: Rescheduled carries no
// direction and no ASNs, Cancelled none of the three, NoShow no ASNs. A row
// built from an event that arrived before its Booked is therefore PARTIAL: the
// facts it lacks (direction, ASN count) are filled in by the Booked later
// (Resolve), and it is not demand until the direction is known to be INBOUND.
// A CANCELLED / NO_SHOW appointment whose booking was never seen has no
// window: it only exists to make a later (older) Booked not resurrect it.
type Appointment struct {
	id          string
	location    string
	direction   Direction
	status      Status
	windowStart time.Time
	windowEnd   time.Time
	asnCount    int
	version     int64
	asOf        time.Time
}

// BookedParams carries everything NewBooked needs.
type BookedParams struct {
	AppointmentID string
	// Location is the event's site_id (a facility-layout siteCode).
	Location    string
	Direction   Direction
	WindowStart time.Time
	WindowEnd   time.Time
	// ASNCount is len(data.asn_numbers); at least 1 for an INBOUND booking.
	ASNCount int
	// Version is the aggregate version of the event (starts at 1).
	Version int64
	// AsOf is the CloudEvents `time` of the event.
	AsOf time.Time
}

func checkKey(id, location string, version int64, asOf time.Time) error {
	if id == "" || location == "" {
		return ErrRequiredField
	}
	if version < 1 {
		return ErrVersionRequired
	}
	if asOf.IsZero() {
		return ErrAsOfRequired
	}
	return nil
}

func checkWindow(start, end time.Time) error {
	if start.IsZero() || !end.After(start) {
		return ErrInvalidWindow
	}
	return nil
}

// NewBooked validates p and builds a BOOKED Appointment. Instants are
// normalized to UTC.
func NewBooked(p BookedParams) (Appointment, error) {
	if err := checkKey(p.AppointmentID, p.Location, p.Version, p.AsOf); err != nil {
		return Appointment{}, err
	}
	if !p.Direction.Valid() {
		return Appointment{}, ErrInvalidDirection
	}
	if err := checkWindow(p.WindowStart, p.WindowEnd); err != nil {
		return Appointment{}, err
	}
	if p.Direction == DirectionInbound && p.ASNCount < 1 {
		return Appointment{}, ErrASNRequired
	}
	return Appointment{
		id: p.AppointmentID, location: p.Location, direction: p.Direction, status: StatusBooked,
		windowStart: p.WindowStart.UTC(), windowEnd: p.WindowEnd.UTC(), asnCount: p.ASNCount,
		version: p.Version, asOf: p.AsOf.UTC(),
	}, nil
}

// NewRescheduled validates its arguments and builds the BOOKED, PARTIAL
// Appointment a DockAppointmentRescheduled carries: the new window, with the
// direction and ASN count unknown until the Booked is seen.
func NewRescheduled(appointmentID, location string, windowStart, windowEnd time.Time, version int64, asOf time.Time) (Appointment, error) {
	if err := checkKey(appointmentID, location, version, asOf); err != nil {
		return Appointment{}, err
	}
	if err := checkWindow(windowStart, windowEnd); err != nil {
		return Appointment{}, err
	}
	return Appointment{
		id: appointmentID, location: location, status: StatusBooked,
		windowStart: windowStart.UTC(), windowEnd: windowEnd.UTC(), version: version, asOf: asOf.UTC(),
	}, nil
}

// NewCancelled validates its arguments and builds a CANCELLED Appointment
// carrying no window, direction or ASN count (a tombstone until merged with
// the booking by Resolve).
func NewCancelled(appointmentID, location string, version int64, asOf time.Time) (Appointment, error) {
	if err := checkKey(appointmentID, location, version, asOf); err != nil {
		return Appointment{}, err
	}
	return Appointment{id: appointmentID, location: location, status: StatusCancelled, version: version, asOf: asOf.UTC()}, nil
}

// NewNoShow validates its arguments and builds a NO_SHOW Appointment. The event
// names the direction; it carries no window and no ASN count.
func NewNoShow(appointmentID, location string, direction Direction, version int64, asOf time.Time) (Appointment, error) {
	if err := checkKey(appointmentID, location, version, asOf); err != nil {
		return Appointment{}, err
	}
	if !direction.Valid() {
		return Appointment{}, ErrInvalidDirection
	}
	return Appointment{id: appointmentID, location: location, direction: direction, status: StatusNoShow, version: version, asOf: asOf.UTC()}, nil
}

// RehydrateParams is the stored state of an appointment, used by repository
// adapters to rebuild it without re-validating.
type RehydrateParams struct {
	AppointmentID string
	Location      string
	// Direction is empty for a partial row (a Rescheduled/Cancelled seen
	// before the Booked).
	Direction Direction
	Status    Status
	// WindowStart, WindowEnd and ASNCount are the zero values when unknown.
	WindowStart time.Time
	WindowEnd   time.Time
	ASNCount    int
	// Version is 0 for a row written before dock-scheduling was the source.
	Version int64
	AsOf    time.Time
}

// Rehydrate rebuilds an Appointment from persisted state. It is for
// repository adapters only; the consumer goes through the New* constructors.
func Rehydrate(p RehydrateParams) Appointment {
	a := Appointment{
		id: p.AppointmentID, location: p.Location, direction: p.Direction, status: p.Status,
		asnCount: p.ASNCount, version: p.Version, asOf: p.AsOf.UTC(),
	}
	if !p.WindowStart.IsZero() {
		a.windowStart, a.windowEnd = p.WindowStart.UTC(), p.WindowEnd.UTC()
	}
	return a
}

// ID returns the dock-scheduling appointment id.
func (a Appointment) ID() string { return a.id }

// Location returns the site (the event's site_id) the appointment's demand is
// attributed to.
func (a Appointment) Location() string { return a.location }

// Direction returns INBOUND or OUTBOUND; empty while only a Rescheduled or a
// Cancelled has been seen.
func (a Appointment) Direction() Direction { return a.direction }

// Status returns BOOKED, CANCELLED or NO_SHOW.
func (a Appointment) Status() Status { return a.status }

// WindowStart returns the start of the door window (UTC); the zero time when
// unknown.
func (a Appointment) WindowStart() time.Time { return a.windowStart }

// WindowEnd returns the end of the door window (UTC); the zero time when
// unknown.
func (a Appointment) WindowEnd() time.Time { return a.windowEnd }

// ASNCount returns how many ASNs the appointment covers; 0 when unknown or
// OUTBOUND.
func (a Appointment) ASNCount() int { return a.asnCount }

// Version returns the aggregate version of the last accepted state change
// (0 for a row that predates dock-scheduling as the source).
func (a Appointment) Version() int64 { return a.version }

// AsOf returns the time of the producer event that last wrote the
// appointment (UTC).
func (a Appointment) AsOf() time.Time { return a.asOf }

// HasWindow reports whether the window is known.
func (a Appointment) HasWindow() bool { return !a.windowStart.IsZero() }

// CountsIn reports whether the appointment is demand in the half-open window
// [start, end): it is INBOUND, BOOKED and its door window STARTS at or after
// start and strictly before end. A start exactly at `start` counts, exactly at
// `end` belongs to the next window, so adjacent windows never count an
// appointment twice (docs/adr/0003, docs/adr/0017 decision 4).
func (a Appointment) CountsIn(start, end time.Time) bool {
	return a.direction == DirectionInbound && a.status == StatusBooked && a.HasWindow() &&
		a.windowStart.Compare(start) >= 0 && a.windowStart.Compare(end) < 0
}

// Resolve decides what the read model stores when next arrives and prev (nil
// when the id is new) is already stored. It is the ordering guard of
// docs/adr/0020 and makes Booked, Rescheduled, Cancelled and NoShow
// order-independent:
//
//   - nothing stored: next is stored (a partial row when it is not a Booked);
//   - the STATE (status, window, version, as-of, site) changes only when
//     next.version > prev.version -- a redelivery, a duplicate or an older
//     event changes nothing; a Cancelled / NoShow keeps the window already
//     known so the row still describes what was removed;
//   - the immutable FACTS (direction, ASN count) are filled in from whichever
//     event carries them while the row still lacks them, so a Booked (version
//     1) arriving after the Rescheduled (version 2) completes the row without
//     moving its window back. The window of a CANCELLED / NO_SHOW row is
//     informational (what was removed) and may differ with the arrival order;
//     the demand never does.
//
// The second result reports whether the model changed.
func Resolve(prev *Appointment, next Appointment) (Appointment, bool) {
	if prev == nil {
		return next, true
	}
	out, changed := *prev, false
	if out.direction == "" && next.direction != "" {
		out.direction, changed = next.direction, true
	}
	if out.asnCount == 0 && next.asnCount > 0 {
		out.asnCount, changed = next.asnCount, true
	}
	if next.version > prev.version {
		out.location, out.status, out.version, out.asOf = next.location, next.status, next.version, next.asOf
		if next.HasWindow() {
			out.windowStart, out.windowEnd = next.windowStart, next.windowEnd
		}
		// A winning Booked also restates the booking facts, so it supersedes
		// the ones a version-0 (inbound-receiving era) row carries.
		if next.direction != "" {
			out.direction = next.direction
		}
		if next.asnCount > 0 {
			out.asnCount = next.asnCount
		}
		changed = true
	}
	return out, changed
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
