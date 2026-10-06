package processcapacity

import (
	"errors"
	"slices"
)

// ProcessType names a warehouse process (e.g. "PICK", "PACK", "REBIN").
// Kept as a thin string type rather than a closed enum -- this context
// treats it as an opaque identifier. Process steps are declared locally by
// operators (ProcessPath, ADR 0001 Addendum); nothing is consumed from
// process-path-management.
type ProcessType string

// ConstraintType names one of the seven limiting factors a ProcessCapacity
// can register a CapacityRate against.
type ConstraintType string

// The seven constraint types this phase supports.
const (
	ConstraintLabor         ConstraintType = "LABOR"
	ConstraintLocation      ConstraintType = "LOCATION"
	ConstraintEquipment     ConstraintType = "EQUIPMENT"
	ConstraintStation       ConstraintType = "STATION"
	ConstraintConveyor      ConstraintType = "CONVEYOR"
	ConstraintBuffer        ConstraintType = "BUFFER"
	ConstraintReplenishment ConstraintType = "REPLENISHMENT"
)

// ErrUnitMismatch is returned by AddConstraint when a rate's unit differs
// from the aggregate's already-established native unit (set by the first
// constraint ever registered). ProcessCapacity never silently force-compares
// across units -- see domain-model.md's ProcessCapacity invariant.
var ErrUnitMismatch = errors.New("processcapacity: constraint rate unit does not match this ProcessCapacity's native unit")

// ErrNoConstraints is returned by EffectiveRate when no constraint has been
// registered yet -- a ProcessCapacity with zero constraints has no
// effective rate to report.
var ErrNoConstraints = errors.New("processcapacity: process capacity has no registered constraints")

// ConstraintEntry pairs a registered constraint type with its rate, in
// registration order. Returned by Constraints() for read-side use (the
// REST GET response, the BDD assertions).
type ConstraintEntry struct {
	Type ConstraintType
	Rate CapacityRate
}

// ProcessCapacity is the aggregate root identified by
// (ProcessType, Location, CapacityWindow): the usable throughput of one
// warehouse process at one location for one time window -- the minimum
// across its registered constraints. See domain-model.md.
type ProcessCapacity struct {
	processType ProcessType
	location    string
	window      CapacityWindow

	nativeUnit    CapacityUnit
	nativeUnitSet bool

	// order preserves constraint registration order (map iteration order
	// in Go is randomized) so EffectiveRate's tie-breaking is deterministic:
	// the earliest-registered constraint wins a tie. Re-registering an
	// existing constraint type updates its rate in place without changing
	// its position in order.
	order       []ConstraintType
	constraints map[ConstraintType]CapacityRate
}

// NewProcessCapacity constructs an empty ProcessCapacity for one
// process + location + window. Constraints are added afterwards via
// AddConstraint.
func NewProcessCapacity(processType ProcessType, location string, window CapacityWindow) *ProcessCapacity {
	return &ProcessCapacity{
		processType: processType,
		location:    location,
		window:      window,
		constraints: make(map[ConstraintType]CapacityRate),
	}
}

// ProcessType returns the aggregate's process type.
func (p *ProcessCapacity) ProcessType() ProcessType { return p.processType }

// Location returns the aggregate's location.
func (p *ProcessCapacity) Location() string { return p.location }

// Window returns the aggregate's capacity window.
func (p *ProcessCapacity) Window() CapacityWindow { return p.window }

// NativeUnit returns the unit every registered constraint shares. The zero
// value (empty string) is returned if no constraint has been registered yet.
func (p *ProcessCapacity) NativeUnit() CapacityUnit { return p.nativeUnit }

// AddConstraint registers (or upserts) one named CapacityRate on this
// ProcessCapacity. The FIRST constraint ever added establishes the
// aggregate's native unit; every subsequent constraint must share that same
// unit or AddConstraint returns ErrUnitMismatch. Re-registering an existing
// constraintType replaces its prior rate -- it never adds a duplicate.
func (p *ProcessCapacity) AddConstraint(constraintType ConstraintType, rate CapacityRate) error {
	if !p.nativeUnitSet {
		p.nativeUnit = rate.Unit()
		p.nativeUnitSet = true
	} else if rate.Unit() != p.nativeUnit {
		return ErrUnitMismatch
	}

	if _, exists := p.constraints[constraintType]; !exists {
		p.order = append(p.order, constraintType)
	}
	p.constraints[constraintType] = rate
	return nil
}

// Constraints returns every registered constraint in registration order.
func (p *ProcessCapacity) Constraints() []ConstraintEntry {
	entries := make([]ConstraintEntry, 0, len(p.order))
	for _, t := range p.order {
		entries = append(entries, ConstraintEntry{Type: t, Rate: p.constraints[t]})
	}
	return entries
}

// EffectiveRate returns the minimum CapacityRate across every registered
// constraint, and which ConstraintType is binding (the one that produced
// that minimum). Returns ErrNoConstraints if nothing has been registered.
// Ties are broken in favor of the earliest-registered constraint.
func (p *ProcessCapacity) EffectiveRate() (CapacityRate, ConstraintType, error) {
	if len(p.order) == 0 {
		return CapacityRate{}, "", ErrNoConstraints
	}

	bindingType := p.order[0]
	minRate := p.constraints[bindingType]
	for _, t := range p.order[1:] {
		candidate := p.constraints[t]
		if candidate.LessThan(minRate) {
			minRate = candidate
			bindingType = t
		}
	}
	return minRate, bindingType, nil
}

// SortNewestFirst sorts pcs in place by window, newest first: the LATER
// window start first and, on equal starts, the narrower window (earlier end)
// first. This is the precedence order of covering aggregates when a step's
// constraints are composed (see ComposeStepCapacity and docs/adr/0003); the
// ProcessCapacityRepository.FindCovering implementations return this order.
func SortNewestFirst(pcs []*ProcessCapacity) {
	slices.SortStableFunc(pcs, func(a, b *ProcessCapacity) int {
		return compareNewestFirst(a.window, b.window)
	})
}
