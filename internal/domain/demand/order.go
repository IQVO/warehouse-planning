// Package demand holds the expected-demand read model's domain rules: one
// Order per order-management order id (last-writer-wins on the event time),
// and the half-open window predicate that decides whether an order counts
// as demand in a planning window. See docs/adr/0004-demand-ingestion-from-
// order-management.md for what an event can and cannot support.
//
// The package performs NO I/O and imports nothing from this module: it is
// pure rules over values handed in by the Kafka consumer and the read use
// cases.
package demand

import (
	"errors"
	"time"
)

var (
	// ErrRequiredField is returned when the order id or the site is blank.
	ErrRequiredField = errors.New("demand: order id and location are required")

	// ErrPromiseRequired is returned when the order carries no promise
	// instant: without it the order cannot be placed in any window.
	ErrPromiseRequired = errors.New("demand: the order's promise instant is required")

	// ErrAsOfRequired is returned when the event time is missing: it is the
	// last-writer-wins key, so an order without it cannot be ordered
	// against another write.
	ErrAsOfRequired = errors.New("demand: the event time (as-of) is required")

	// ErrNegativeLines is returned for a negative released-line count.
	ErrNegativeLines = errors.New("demand: released lines must not be negative")
)

// Order is one order-management order as the expected-demand read model
// knows it: which site its demand is attributed to, the instant by which it
// is promised (the cutoff it must ship by), how many lines the latest
// allocation pass released, and the time of the event that last wrote it.
type Order struct {
	id            string
	location      string
	promiseAt     time.Time
	releasedLines int
	asOf          time.Time
}

// OrderParams carries everything NewOrder needs.
type OrderParams struct {
	OrderID  string
	Location string
	// PromiseAt is the order's promise cutoff (order-management's
	// promise_date).
	PromiseAt time.Time
	// ReleasedLines is the number of lines the latest allocation pass
	// released (len(data.lines)); informational, may be zero.
	ReleasedLines int
	// AsOf is the CloudEvents `time` of the event that produced this write.
	AsOf time.Time
}

// NewOrder validates p and builds an Order. Both instants are normalized to
// UTC.
func NewOrder(p OrderParams) (Order, error) {
	if p.OrderID == "" || p.Location == "" {
		return Order{}, ErrRequiredField
	}
	if p.PromiseAt.IsZero() {
		return Order{}, ErrPromiseRequired
	}
	if p.AsOf.IsZero() {
		return Order{}, ErrAsOfRequired
	}
	if p.ReleasedLines < 0 {
		return Order{}, ErrNegativeLines
	}
	return Order{
		id:            p.OrderID,
		location:      p.Location,
		promiseAt:     p.PromiseAt.UTC(),
		releasedLines: p.ReleasedLines,
		asOf:          p.AsOf.UTC(),
	}, nil
}

// ID returns the order-management order id.
func (o Order) ID() string { return o.id }

// Location returns the site the order's demand is attributed to.
func (o Order) Location() string { return o.location }

// PromiseAt returns the order's promise cutoff (UTC).
func (o Order) PromiseAt() time.Time { return o.promiseAt }

// ReleasedLines returns the lines released by the latest allocation pass.
func (o Order) ReleasedLines() int { return o.releasedLines }

// AsOf returns the time of the event that last wrote the order (UTC).
func (o Order) AsOf() time.Time { return o.asOf }

// Supersedes reports whether a write of o replaces the stored prev of the
// same order id: last-writer-wins on the event time, where a LATER OR EQUAL
// time replaces and an older one is a no-op. Equal replaces so that
// re-applying a same-time event under a new id converges on the latest
// payload instead of depending on arrival order.
func (o Order) Supersedes(prev Order) bool {
	return o.asOf.Compare(prev.asOf) >= 0
}

// CountsIn reports whether the order is demand in the half-open window
// [start, end): its promise cutoff is at or after start and strictly before
// end. A cutoff exactly at start counts; a cutoff exactly at end belongs to
// the next window, so adjacent windows never count an order twice.
func (o Order) CountsIn(start, end time.Time) bool {
	return o.promiseAt.Compare(start) >= 0 && o.promiseAt.Compare(end) < 0
}

// Summary is the expected demand of one site over one window.
type Summary struct {
	// Orders is the number of distinct orders whose promise cutoff falls in
	// the window.
	Orders int
	// ReleasedLines sums those orders' released-line counts. It is NOT
	// units: order-management's events carry no quantities.
	ReleasedLines int
	// AsOf is the newest event time the site's model reflects (any order,
	// in or out of the window); the zero time when the site has no orders.
	AsOf time.Time
}

// HasData reports whether the window holds at least one order. A window
// without orders is "no data", never "zero demand": an empty answer must
// not silently become a plan with zero demand.
func (s Summary) HasData() bool { return s.Orders > 0 }

// Summarize counts the orders of ONE site that fall in [start, end) and
// reports the newest as-of across all of them. It is the reference
// implementation the in-memory repository uses; the Postgres adapter
// computes the same figures in SQL and is checked against the same
// boundary cases.
func Summarize(orders []Order, start, end time.Time) Summary {
	var s Summary
	for _, o := range orders {
		if o.asOf.After(s.AsOf) {
			s.AsOf = o.asOf
		}
		if o.CountsIn(start, end) {
			s.Orders++
			s.ReleasedLines += o.releasedLines
		}
	}
	return s
}
