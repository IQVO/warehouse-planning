package processcapacity

import "errors"

// ErrNonPositiveConversionFactor is returned when a WorkloadProfile
// conversion factor is set to zero or negative. A zero or negative factor
// would silently divide-by-zero or invert every comparison built on top of
// it, so the constructor rejects it up front rather than deferring the
// failure to whichever rate normalization happens to use it first.
var ErrNonPositiveConversionFactor = errors.New("processcapacity: workload profile conversion factor must be positive")

// ErrMissingConversionFactor is returned by NormalizeToOrderRate when the
// CapacityRate being normalized is in a unit this WorkloadProfile has no
// conversion factor configured for.
var ErrMissingConversionFactor = errors.New("processcapacity: workload profile has no conversion factor for this rate's unit")

// ErrUnsupportedNormalizationUnit is returned by NormalizeToOrderRate when
// the rate's unit has no defined ORDER-normalization path at all (LINE, as
// of this phase -- see domain-model.md).
var ErrUnsupportedNormalizationUnit = errors.New("processcapacity: workload profile cannot normalize this rate's unit to ORDER")

// WorkloadProfile holds the per-warehouse conversion factors used to
// normalize different processes' native CapacityRate units into one
// comparable flow unit, ORDER (see domain-model.md). Each factor is
// independently optional -- a profile only needs the factors its callers'
// ProcessPath steps actually use (a path made only of UNIT-measured steps
// never needs PackagesPerOrder) -- but whichever factor IS set must be
// strictly positive. Immutable once constructed.
type WorkloadProfile struct {
	unitsPerOrder    float64
	unitsPerOrderSet bool

	packagesPerOrder    float64
	packagesPerOrderSet bool
}

// NewWorkloadProfile constructs a WorkloadProfile from optional conversion
// factors. A nil pointer means "this factor is not configured"; a non-nil
// pointer to a zero or negative value is rejected with
// ErrNonPositiveConversionFactor.
func NewWorkloadProfile(unitsPerOrder, packagesPerOrder *float64) (WorkloadProfile, error) {
	profile := WorkloadProfile{}

	if unitsPerOrder != nil {
		if *unitsPerOrder <= 0 {
			return WorkloadProfile{}, ErrNonPositiveConversionFactor
		}
		profile.unitsPerOrder = *unitsPerOrder
		profile.unitsPerOrderSet = true
	}

	if packagesPerOrder != nil {
		if *packagesPerOrder <= 0 {
			return WorkloadProfile{}, ErrNonPositiveConversionFactor
		}
		profile.packagesPerOrder = *packagesPerOrder
		profile.packagesPerOrderSet = true
	}

	return profile, nil
}

// UnitsPerOrder returns the configured UNIT-per-ORDER factor and whether it
// was set.
func (p WorkloadProfile) UnitsPerOrder() (float64, bool) { return p.unitsPerOrder, p.unitsPerOrderSet }

// PackagesPerOrder returns the configured PACKAGE-per-ORDER factor and
// whether it was set.
func (p WorkloadProfile) PackagesPerOrder() (float64, bool) {
	return p.packagesPerOrder, p.packagesPerOrderSet
}

// NormalizeToOrderRate converts rate -- expressed in UNIT, PACKAGE or
// already ORDER -- into the equivalent ORDER rate over the SAME period, by
// dividing rate's quantity by this profile's matching conversion factor
// (e.g. 4000 UNIT/HOUR with UnitsPerOrder=2.5 -> 1600 ORDER/HOUR). An
// ORDER rate passes through unchanged. Returns ErrMissingConversionFactor
// if the needed factor was never configured on this profile, or
// ErrUnsupportedNormalizationUnit for a unit this phase has no
// normalization path for at all (LINE).
func (p WorkloadProfile) NormalizeToOrderRate(rate CapacityRate) (CapacityRate, error) {
	switch rate.Unit() {
	case UnitOrder:
		return rate, nil
	case UnitUnit:
		if !p.unitsPerOrderSet {
			return CapacityRate{}, ErrMissingConversionFactor
		}
		return NewCapacityRate(rate.Quantity()/p.unitsPerOrder, UnitOrder, rate.Period())
	case UnitPackage:
		if !p.packagesPerOrderSet {
			return CapacityRate{}, ErrMissingConversionFactor
		}
		return NewCapacityRate(rate.Quantity()/p.packagesPerOrder, UnitOrder, rate.Period())
	default:
		return CapacityRate{}, ErrUnsupportedNormalizationUnit
	}
}
