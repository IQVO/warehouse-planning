package processcapacity

import (
	"errors"
	"testing"
	"time"
)

// f64ptr is a small helper so fixtures can express "this optional factor
// is set to exactly this value" without a throwaway local variable at
// every call site.
func f64ptr(v float64) *float64 { return &v }

func mustWorkloadProfile(t *testing.T, unitsPerOrder, packagesPerOrder *float64) WorkloadProfile {
	t.Helper()
	profile, err := NewWorkloadProfile(unitsPerOrder, packagesPerOrder)
	if err != nil {
		t.Fatalf("unexpected error building fixture WorkloadProfile: %v", err)
	}
	return profile
}

func TestNewWorkloadProfile_AcceptsNilFactors(t *testing.T) {
	profile, err := NewWorkloadProfile(nil, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if _, ok := profile.UnitsPerOrder(); ok {
		t.Fatalf("expected UnitsPerOrder to be unset")
	}
	if _, ok := profile.PackagesPerOrder(); ok {
		t.Fatalf("expected PackagesPerOrder to be unset")
	}
}

// TestNewWorkloadProfile_RejectsZeroUnitsPerOrder exercises the boundary
// value itself (0), not just a clearly-negative number -- a test that only
// tries -1 would let a `<=` -> `<` mutant on the guard survive silently
// (see .claude/skills/how-to-test.md's boundary-guard pitfall).
func TestNewWorkloadProfile_RejectsZeroUnitsPerOrder(t *testing.T) {
	_, err := NewWorkloadProfile(f64ptr(0), nil)
	if !errors.Is(err, ErrNonPositiveConversionFactor) {
		t.Fatalf("expected ErrNonPositiveConversionFactor, got %v", err)
	}
}

func TestNewWorkloadProfile_RejectsNegativeUnitsPerOrder(t *testing.T) {
	_, err := NewWorkloadProfile(f64ptr(-2.5), nil)
	if !errors.Is(err, ErrNonPositiveConversionFactor) {
		t.Fatalf("expected ErrNonPositiveConversionFactor, got %v", err)
	}
}

func TestNewWorkloadProfile_RejectsZeroPackagesPerOrder(t *testing.T) {
	_, err := NewWorkloadProfile(nil, f64ptr(0))
	if !errors.Is(err, ErrNonPositiveConversionFactor) {
		t.Fatalf("expected ErrNonPositiveConversionFactor, got %v", err)
	}
}

func TestNewWorkloadProfile_RejectsNegativePackagesPerOrder(t *testing.T) {
	_, err := NewWorkloadProfile(nil, f64ptr(-1))
	if !errors.Is(err, ErrNonPositiveConversionFactor) {
		t.Fatalf("expected ErrNonPositiveConversionFactor, got %v", err)
	}
}

func TestNewWorkloadProfile_AcceptsPositiveFactors(t *testing.T) {
	profile := mustWorkloadProfile(t, f64ptr(2.5), f64ptr(1))
	if got, ok := profile.UnitsPerOrder(); !ok || got != 2.5 {
		t.Fatalf("expected UnitsPerOrder 2.5 (set), got %v (set=%v)", got, ok)
	}
	if got, ok := profile.PackagesPerOrder(); !ok || got != 1 {
		t.Fatalf("expected PackagesPerOrder 1 (set), got %v (set=%v)", got, ok)
	}
}

// TestWorkloadProfile_NormalizeToOrderRate_WorkedExample reproduces,
// byte-for-byte, the design doc's normalization worked example: 4000
// UNIT/HOUR with UnitsPerOrder=2.5 -> 1600 ORDER/HOUR, and 1800
// PACKAGE/HOUR with PackagesPerOrder=1 -> 1800 ORDER/HOUR.
func TestWorkloadProfile_NormalizeToOrderRate_WorkedExample(t *testing.T) {
	profile := mustWorkloadProfile(t, f64ptr(2.5), f64ptr(1))

	unitRate := mustRate(t, 4000, UnitUnit, time.Hour)
	normalizedUnit, err := profile.NormalizeToOrderRate(unitRate)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if normalizedUnit.Quantity() != 1600 || normalizedUnit.Unit() != UnitOrder || normalizedUnit.Period() != time.Hour {
		t.Fatalf("expected 1600 ORDER/HOUR, got %v %v/%v", normalizedUnit.Quantity(), normalizedUnit.Unit(), normalizedUnit.Period())
	}

	packageRate := mustRate(t, 1800, UnitPackage, time.Hour)
	normalizedPackage, err := profile.NormalizeToOrderRate(packageRate)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if normalizedPackage.Quantity() != 1800 || normalizedPackage.Unit() != UnitOrder || normalizedPackage.Period() != time.Hour {
		t.Fatalf("expected 1800 ORDER/HOUR, got %v %v/%v", normalizedPackage.Quantity(), normalizedPackage.Unit(), normalizedPackage.Period())
	}
}

func TestWorkloadProfile_NormalizeToOrderRate_OrderPassesThroughUnchanged(t *testing.T) {
	profile := mustWorkloadProfile(t, nil, nil)
	orderRate := mustRate(t, 900, UnitOrder, time.Hour)
	got, err := profile.NormalizeToOrderRate(orderRate)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got != orderRate {
		t.Fatalf("expected an ORDER rate to pass through unchanged, got %v", got)
	}
}

func TestWorkloadProfile_NormalizeToOrderRate_MissingUnitsFactor(t *testing.T) {
	profile := mustWorkloadProfile(t, nil, f64ptr(1))
	_, err := profile.NormalizeToOrderRate(mustRate(t, 4000, UnitUnit, time.Hour))
	if !errors.Is(err, ErrMissingConversionFactor) {
		t.Fatalf("expected ErrMissingConversionFactor, got %v", err)
	}
}

func TestWorkloadProfile_NormalizeToOrderRate_MissingPackagesFactor(t *testing.T) {
	profile := mustWorkloadProfile(t, f64ptr(2.5), nil)
	_, err := profile.NormalizeToOrderRate(mustRate(t, 1800, UnitPackage, time.Hour))
	if !errors.Is(err, ErrMissingConversionFactor) {
		t.Fatalf("expected ErrMissingConversionFactor, got %v", err)
	}
}

func TestWorkloadProfile_NormalizeToOrderRate_UnsupportedUnit(t *testing.T) {
	profile := mustWorkloadProfile(t, f64ptr(2.5), f64ptr(1))
	_, err := profile.NormalizeToOrderRate(mustRate(t, 100, UnitLine, time.Hour))
	if !errors.Is(err, ErrUnsupportedNormalizationUnit) {
		t.Fatalf("expected ErrUnsupportedNormalizationUnit, got %v", err)
	}
}
