// Package processcapacity holds the ProcessCapacity aggregate and its
// supporting value objects: CapacityRate, CapacityWindow. See
// .claude/rules/domain-model.md for the ubiquitous language.
package processcapacity

import (
	"errors"
	"time"
)

// CapacityUnit is the native unit a CapacityRate is expressed in. Rates are
// never compared across differing units without going through a
// WorkloadProfile first (a later phase) -- see domain-model.md.
type CapacityUnit string

// The four native units this phase supports.
const (
	UnitUnit    CapacityUnit = "UNIT"
	UnitLine    CapacityUnit = "LINE"
	UnitOrder   CapacityUnit = "ORDER"
	UnitPackage CapacityUnit = "PACKAGE"
)

// ErrNegativeQuantity is returned when a CapacityRate is constructed with a
// negative quantity. Zero is a valid (if degenerate) rate -- it legitimately
// represents "no throughput" for a constraint.
var ErrNegativeQuantity = errors.New("processcapacity: capacity rate quantity must not be negative")

// ErrNonPositivePeriod is returned when a CapacityRate's period is zero or
// negative -- a rate is meaningless without a positive time base.
var ErrNonPositivePeriod = errors.New("processcapacity: capacity rate period must be positive")

// CapacityRate is a quantity + native unit + period value object, e.g.
// "4000 UNIT / HOUR". Immutable once constructed.
type CapacityRate struct {
	quantity float64
	unit     CapacityUnit
	period   time.Duration
}

// NewCapacityRate constructs a CapacityRate, rejecting a negative quantity
// or a non-positive period. Zero quantity is accepted.
func NewCapacityRate(quantity float64, unit CapacityUnit, period time.Duration) (CapacityRate, error) {
	if quantity < 0 {
		return CapacityRate{}, ErrNegativeQuantity
	}
	if period <= 0 {
		return CapacityRate{}, ErrNonPositivePeriod
	}
	return CapacityRate{quantity: quantity, unit: unit, period: period}, nil
}

// Quantity returns the rate's raw quantity.
func (r CapacityRate) Quantity() float64 { return r.quantity }

// Unit returns the rate's native unit.
func (r CapacityRate) Unit() CapacityUnit { return r.unit }

// Period returns the rate's time base.
func (r CapacityRate) Period() time.Duration { return r.period }

// perSecond normalizes the rate to a quantity-per-second figure so two
// rates expressed over different periods (e.g. 100/hour vs 100/30min) can be
// compared directly.
func (r CapacityRate) perSecond() float64 {
	return r.quantity / r.period.Seconds()
}

// LessThan reports whether r represents a smaller throughput than other,
// comparing both rates' per-second equivalent (quantity / period.Seconds()).
//
// LessThan assumes both rates share the same native Unit -- comparing a
// UNIT/HOUR rate against a PACKAGE/HOUR rate produces a numeric answer that
// is not domain-meaningful. Enforcing that invariant is the caller's
// responsibility (see ProcessCapacity.AddConstraint, which rejects a
// mismatched unit before a rate ever reaches this comparison).
func (r CapacityRate) LessThan(other CapacityRate) bool {
	return r.perSecond() < other.perSecond()
}
