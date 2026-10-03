package processcapacity

import (
	"errors"
	"testing"
	"time"

	"github.com/claudioed/warehouse-planning/internal/domain/processpath"
)

func mustProcessPath(t *testing.T, id, name string, steps []processpath.ProcessType) processpath.ProcessPath {
	t.Helper()
	path, err := processpath.NewProcessPath(id, name, steps)
	if err != nil {
		t.Fatalf("unexpected error building fixture ProcessPath: %v", err)
	}
	return path
}

// pickRebinPackCapacities builds the design doc's worked example:
// Pick=4000 UNIT/HOUR, Rebin=2500 UNIT/HOUR, Pack=1800 PACKAGE/HOUR, all at
// PATH-ZONE-A over the pickZoneAWindow fixture window.
func pickRebinPackCapacities(t *testing.T) map[ProcessType]*ProcessCapacity {
	t.Helper()
	window := pickZoneAWindow(t)

	pick := NewProcessCapacity("PICK", "PATH-ZONE-A", window)
	if err := pick.AddConstraint(ConstraintLabor, mustRate(t, 4000, UnitUnit, time.Hour)); err != nil {
		t.Fatalf("unexpected error adding PICK's constraint: %v", err)
	}

	rebin := NewProcessCapacity("REBIN", "PATH-ZONE-A", window)
	if err := rebin.AddConstraint(ConstraintLabor, mustRate(t, 2500, UnitUnit, time.Hour)); err != nil {
		t.Fatalf("unexpected error adding REBIN's constraint: %v", err)
	}

	pack := NewProcessCapacity("PACK", "PATH-ZONE-A", window)
	if err := pack.AddConstraint(ConstraintLabor, mustRate(t, 1800, UnitPackage, time.Hour)); err != nil {
		t.Fatalf("unexpected error adding PACK's constraint: %v", err)
	}

	return map[ProcessType]*ProcessCapacity{
		"PICK":  pick,
		"REBIN": rebin,
		"PACK":  pack,
	}
}

// TestComputeProcessPathCapacity_WorkedExample reproduces, byte-for-byte,
// the design doc's Pick -> Rebin -> Pack worked example: native capacities
// Pick=4000 UNIT/HOUR, Rebin=2500 UNIT/HOUR, Pack=1800 PACKAGE/HOUR with
// WorkloadProfile{UnitsPerOrder:2.5, PackagesPerOrder:1} normalize to
// Pick=1600, Rebin=1000, Pack=1800 (all ORDER/HOUR) -> the path capacity is
// 1000 ORDER/HOUR with Rebin as the bottleneck. Kept forever as a
// regression fixture -- do not change the numbers below without updating
// the design doc too.
func TestComputeProcessPathCapacity_WorkedExample(t *testing.T) {
	path := mustProcessPath(t, "pick-rebin-pack", "Pick-Rebin-Pack", []processpath.ProcessType{"PICK", "REBIN", "PACK"})
	capacities := pickRebinPackCapacities(t)
	profile := mustWorkloadProfile(t, f64ptr(2.5), f64ptr(1))

	rate, bottleneck, err := ComputeProcessPathCapacity(path, capacities, profile)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if bottleneck != "REBIN" {
		t.Fatalf("expected REBIN to be the bottleneck, got %v", bottleneck)
	}
	if rate.Quantity() != 1000 || rate.Unit() != UnitOrder || rate.Period() != time.Hour {
		t.Fatalf("expected 1000 ORDER/HOUR, got %v %v/%v", rate.Quantity(), rate.Unit(), rate.Period())
	}
}

// TestComputeProcessPathCapacity_MissingStepCapacity proves a step absent
// from the capacities map produces a clear, named error -- never a false
// zero bottleneck and never a silently-skipped step.
func TestComputeProcessPathCapacity_MissingStepCapacity(t *testing.T) {
	path := mustProcessPath(t, "pick-rebin-pack", "Pick-Rebin-Pack", []processpath.ProcessType{"PICK", "REBIN", "PACK"})
	capacities := pickRebinPackCapacities(t)
	delete(capacities, "REBIN")
	profile := mustWorkloadProfile(t, f64ptr(2.5), f64ptr(1))

	_, _, err := ComputeProcessPathCapacity(path, capacities, profile)
	if !errors.Is(err, ErrMissingStepCapacity) {
		t.Fatalf("expected ErrMissingStepCapacity, got %v", err)
	}
	if want := "processcapacity: missing capacity data for step: REBIN"; err.Error() != want {
		t.Fatalf("expected error message %q, got %q", want, err.Error())
	}
}

// TestComputeProcessPathCapacity_FirstStepIsTheBottleneckWhenSmallest
// exercises the i==0 branch of the running-minimum loop with a fixture
// where the FIRST step (not a later one) is the true minimum -- without
// this, a mutant flipping `i == 0` to never seed the running minimum could
// still pass the worked example above (whose minimum happens to be the
// middle step).
func TestComputeProcessPathCapacity_FirstStepIsTheBottleneckWhenSmallest(t *testing.T) {
	window := pickZoneAWindow(t)

	pick := NewProcessCapacity("PICK", "PATH-ZONE-A", window)
	if err := pick.AddConstraint(ConstraintLabor, mustRate(t, 900, UnitUnit, time.Hour)); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	pack := NewProcessCapacity("PACK", "PATH-ZONE-A", window)
	if err := pack.AddConstraint(ConstraintLabor, mustRate(t, 1800, UnitPackage, time.Hour)); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	path := mustProcessPath(t, "pick-pack", "Pick-Pack", []processpath.ProcessType{"PICK", "PACK"})
	capacities := map[ProcessType]*ProcessCapacity{"PICK": pick, "PACK": pack}
	profile := mustWorkloadProfile(t, f64ptr(2.5), f64ptr(1))

	rate, bottleneck, err := ComputeProcessPathCapacity(path, capacities, profile)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	// Pick normalizes to 900/2.5 = 360 ORDER/HOUR, Pack to 1800 ORDER/HOUR.
	if bottleneck != "PICK" {
		t.Fatalf("expected PICK to be the bottleneck, got %v", bottleneck)
	}
	if rate.Quantity() != 360 {
		t.Fatalf("expected 360 ORDER/HOUR, got %v", rate.Quantity())
	}
}

// TestComputeProcessPathCapacity_PropagatesNormalizationError proves a
// normalization failure (here, a path step whose native unit has no
// matching conversion factor on the profile) is propagated rather than
// swallowed into a false result.
func TestComputeProcessPathCapacity_PropagatesNormalizationError(t *testing.T) {
	window := pickZoneAWindow(t)
	pick := NewProcessCapacity("PICK", "PATH-ZONE-A", window)
	if err := pick.AddConstraint(ConstraintLabor, mustRate(t, 4000, UnitUnit, time.Hour)); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	path := mustProcessPath(t, "pick-only", "Pick-Only", []processpath.ProcessType{"PICK"})
	capacities := map[ProcessType]*ProcessCapacity{"PICK": pick}
	profile := mustWorkloadProfile(t, nil, f64ptr(1)) // no UnitsPerOrder configured

	_, _, err := ComputeProcessPathCapacity(path, capacities, profile)
	if !errors.Is(err, ErrMissingConversionFactor) {
		t.Fatalf("expected ErrMissingConversionFactor, got %v", err)
	}
}
