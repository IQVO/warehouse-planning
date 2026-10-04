package processcapacity

import "errors"

// ErrStationStandardRequiredField is returned when a StationStandard is
// declared with a blank location or process type.
var ErrStationStandardRequiredField = errors.New("processcapacity: station standard location and process type are required")

// ErrNonPositiveStationStandard is returned when the declared throughput per
// station is zero. A station that processes nothing is not a standard, and a
// zero would silently pin every step it composes into to zero capacity;
// negative quantities are rejected by NewCapacityRate itself
// (ErrNegativeQuantity).
var ErrNonPositiveStationStandard = errors.New("processcapacity: station standard throughput per station must be positive")

// StationStandard is the operator-declared throughput of ONE station of a
// process at a site (location): e.g. 180 PACKAGE per hour per PACK station
// (design doc section 19: 10 stations x 180 packages/hour/station = 1,800
// packages/hour). It is a planning parameter owned by this bounded context --
// no upstream context publishes it -- keyed by (location, process type).
//
// A station COUNT (tallied from facility-layout) has no throughput of its
// own; only count x StationStandard does. See ADR 0002. Immutable once
// constructed.
type StationStandard struct {
	location    string
	processType ProcessType
	perStation  CapacityRate
}

// NewStationStandard constructs a StationStandard, rejecting a blank
// location or process type, a zero throughput (ErrNonPositiveStationStandard)
// and a unit that cannot be normalized to ORDER (ErrUnsupportedNormalizationUnit:
// anything but UNIT, PACKAGE or ORDER -- a LINE or unknown unit could never be
// compared against the other candidates of a step).
func NewStationStandard(location string, processType ProcessType, perStation CapacityRate) (StationStandard, error) {
	if location == "" || processType == "" {
		return StationStandard{}, ErrStationStandardRequiredField
	}
	if perStation.Quantity() <= 0 {
		return StationStandard{}, ErrNonPositiveStationStandard
	}
	switch perStation.Unit() {
	case UnitUnit, UnitPackage, UnitOrder:
	default:
		return StationStandard{}, ErrUnsupportedNormalizationUnit
	}
	return StationStandard{location: location, processType: processType, perStation: perStation}, nil
}

// Location returns the site the standard applies to.
func (s StationStandard) Location() string { return s.location }

// ProcessType returns the process the standard applies to.
func (s StationStandard) ProcessType() ProcessType { return s.processType }

// PerStation returns the throughput of one station.
func (s StationStandard) PerStation() CapacityRate { return s.perStation }

// CapacityFor returns the throughput of stationCount stations: the per-station
// quantity multiplied by the count, in the same unit and over the same
// period. A count below zero yields ErrNegativeQuantity (NewCapacityRate).
func (s StationStandard) CapacityFor(stationCount int) (CapacityRate, error) {
	return NewCapacityRate(s.perStation.Quantity()*float64(stationCount), s.perStation.Unit(), s.perStation.Period())
}
