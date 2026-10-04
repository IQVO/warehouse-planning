package processcapacity

import (
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/claudioed/warehouse-planning/internal/domain/processpath"
)

// --- fixtures (design doc section 19: 10 stations x 180 packages/hour) -------

func mustStandard(t *testing.T, location string, process ProcessType, qty float64, unit CapacityUnit) *StationStandard {
	t.Helper()
	std, err := NewStationStandard(location, process, mustRate(t, qty, unit, time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	return &std
}

func laborAt(t *testing.T, process ProcessType, location string, qty float64, unit CapacityUnit) *ProcessCapacity {
	t.Helper()
	pc := NewProcessCapacity(process, location, pickZoneAWindow(t))
	if err := pc.AddConstraint(ConstraintLabor, mustRate(t, qty, unit, time.Hour)); err != nil {
		t.Fatal(err)
	}
	return pc
}

// fixtureAPack is FIXTURE A: site SIM1, 10 PACK stations tallied, standard
// 180 PACKAGE/hour/station, LABOR for PACK = 2500 PACKAGE/hour.
func fixtureAPack(t *testing.T) StepInput {
	return StepInput{
		Registered:   laborAt(t, "PACK", "SIM1", 2500, UnitPackage),
		Location:     "SIM1",
		StationCount: 10,
		Standard:     mustStandard(t, "SIM1", "PACK", 180, UnitPackage),
	}
}

func TestComposeStepCapacity_FixtureA_StationBindsBelowLabor(t *testing.T) {
	profile := mustWorkloadProfile(t, f64ptr(2.5), f64ptr(1))
	got, err := ComposeStepCapacity("PACK", fixtureAPack(t), profile)
	if err != nil {
		t.Fatal(err)
	}
	// 10 x 180 = 1800 PACKAGE/h < LABOR 2500 PACKAGE/h; /1 package per order.
	if got.Rate.Quantity() != 1800 || got.Rate.Unit() != UnitOrder || got.Rate.Period() != time.Hour {
		t.Fatalf("got %v %s per %v, want 1800 ORDER per hour", got.Rate.Quantity(), got.Rate.Unit(), got.Rate.Period())
	}
	if got.Binding != ConstraintStation {
		t.Fatalf("binding = %s, want STATION", got.Binding)
	}
	if got.Step != "PACK" || len(got.Warnings) != 0 {
		t.Fatalf("step %s warnings %v, want PACK and none", got.Step, got.Warnings)
	}
}

// Normalization divides by the profile's factor: with 2 packages per order
// the same step is 900 ORDER/h, not 1800 (kills a multiply/divide mutant).
func TestComposeStepCapacity_NormalizesWithTheProfileFactor(t *testing.T) {
	profile := mustWorkloadProfile(t, f64ptr(2.5), f64ptr(2))
	got, err := ComposeStepCapacity("PACK", fixtureAPack(t), profile)
	if err != nil {
		t.Fatal(err)
	}
	if got.Rate.Quantity() != 900 || got.Binding != ConstraintStation {
		t.Fatalf("got %v bound by %s, want 900 bound by STATION", got.Rate.Quantity(), got.Binding)
	}
}

// Doc rule 8: units/hour and packages/hour are not comparable raw. LABOR is
// 2500 UNIT/h (= 1000 ORDER/h at 2.5 units/order) while the station standard
// yields 1800 PACKAGE/h (= 1800 ORDER/h): raw, 1800 < 2500 would wrongly pick
// STATION; normalized, LABOR (1000) binds.
func TestComposeStepCapacity_NormalizesEveryCandidateBeforeComparing(t *testing.T) {
	in := fixtureAPack(t)
	in.Registered = laborAt(t, "PACK", "SIM1", 2500, UnitUnit)
	got, err := ComposeStepCapacity("PACK", in, mustWorkloadProfile(t, f64ptr(2.5), f64ptr(1)))
	if err != nil {
		t.Fatal(err)
	}
	if got.Rate.Quantity() != 1000 || got.Binding != ConstraintLabor {
		t.Fatalf("got %v bound by %s, want 1000 bound by LABOR", got.Rate.Quantity(), got.Binding)
	}
}

// An ORDER standard passes through normalization unchanged (no factor needed
// for it): 10 x 100 ORDER/h = 1000 < LABOR 4000 UNIT/2.5 = 1600.
func TestComposeStepCapacity_OrderStandardPassesThroughUnchanged(t *testing.T) {
	in := StepInput{
		Registered:   laborAt(t, "PACK", "SIM1", 4000, UnitUnit),
		Location:     "SIM1",
		StationCount: 10,
		Standard:     mustStandard(t, "SIM1", "PACK", 100, UnitOrder),
	}
	got, err := ComposeStepCapacity("PACK", in, mustWorkloadProfile(t, f64ptr(2.5), nil))
	if err != nil {
		t.Fatal(err)
	}
	if got.Rate.Quantity() != 1000 || got.Binding != ConstraintStation {
		t.Fatalf("got %v bound by %s, want 1000 bound by STATION", got.Rate.Quantity(), got.Binding)
	}
}

// FIXTURE C: stations tallied, NO standard declared -> the step is computed
// from LABOR only and an explicit warning is carried; no throughput invented.
func TestComposeStepCapacity_FixtureC_NoStandardWarnsAndUsesLaborOnly(t *testing.T) {
	in := fixtureAPack(t)
	in.Standard = nil
	got, err := ComposeStepCapacity("PACK", in, mustWorkloadProfile(t, f64ptr(2.5), f64ptr(1)))
	if err != nil {
		t.Fatal(err)
	}
	if got.Rate.Quantity() != 2500 || got.Binding != ConstraintLabor {
		t.Fatalf("got %v bound by %s, want 2500 bound by LABOR", got.Rate.Quantity(), got.Binding)
	}
	want := "no station standard declared for PACK at SIM1: 10 stations are tallied but their throughput is unknown, so no STATION constraint was applied"
	if len(got.Warnings) != 1 || got.Warnings[0] != want {
		t.Fatalf("warnings = %q, want [%q]", got.Warnings, want)
	}
}

// A standard with zero tallied stations contributes nothing and warns about
// nothing; one station is the smallest count that derives a constraint.
func TestComposeStepCapacity_StationCountBoundary(t *testing.T) {
	profile := mustWorkloadProfile(t, f64ptr(2.5), f64ptr(1))

	in := fixtureAPack(t)
	in.StationCount = 0
	got, err := ComposeStepCapacity("PACK", in, profile)
	if err != nil {
		t.Fatal(err)
	}
	if got.Rate.Quantity() != 2500 || got.Binding != ConstraintLabor || len(got.Warnings) != 0 {
		t.Fatalf("0 stations: got %v bound by %s warnings %v, want LABOR 2500 and none", got.Rate.Quantity(), got.Binding, got.Warnings)
	}

	in.StationCount = 1
	got, err = ComposeStepCapacity("PACK", in, profile)
	if err != nil {
		t.Fatal(err)
	}
	if got.Rate.Quantity() != 180 || got.Binding != ConstraintStation {
		t.Fatalf("1 station: got %v bound by %s, want STATION 180", got.Rate.Quantity(), got.Binding)
	}

	in.Standard = nil
	got, err = ComposeStepCapacity("PACK", in, profile)
	if err != nil {
		t.Fatal(err)
	}
	if len(got.Warnings) != 1 || !strings.Contains(got.Warnings[0], "1 stations are tallied") {
		t.Fatalf("1 station without standard: warnings %q, want one warning naming the count", got.Warnings)
	}
}

// No registered constraints but a derived STATION constraint: still computable.
func TestComposeStepCapacity_DerivedOnlyIsComputable(t *testing.T) {
	in := fixtureAPack(t)
	in.Registered = nil
	got, err := ComposeStepCapacity("PACK", in, mustWorkloadProfile(t, nil, f64ptr(1)))
	if err != nil {
		t.Fatal(err)
	}
	if got.Rate.Quantity() != 1800 || got.Binding != ConstraintStation {
		t.Fatalf("got %v bound by %s, want 1800 bound by STATION", got.Rate.Quantity(), got.Binding)
	}
}

func TestComposeStepCapacity_NeitherCandidateIsMissingStepCapacity(t *testing.T) {
	profile := mustWorkloadProfile(t, f64ptr(2.5), f64ptr(1))
	cases := map[string]StepInput{
		"nothing at all":             {Location: "SIM1"},
		"stations but no standard":   {Location: "SIM1", StationCount: 4},
		"standard but zero stations": {Location: "SIM1", Standard: mustStandard(t, "SIM1", "PACK", 180, UnitPackage)},
	}
	for name, in := range cases {
		t.Run(name, func(t *testing.T) {
			_, err := ComposeStepCapacity("PACK", in, profile)
			if !errors.Is(err, ErrMissingStepCapacity) {
				t.Fatalf("got %v, want ErrMissingStepCapacity", err)
			}
			if want := "processcapacity: missing capacity data for step: PACK"; err.Error() != want {
				t.Fatalf("message %q, want %q", err.Error(), want)
			}
		})
	}
}

// A registered LINE constraint (what the retired facility consumer used to
// write) still cannot be normalized and is rejected with the existing error.
func TestComposeStepCapacity_RejectsUnnormalizableRegisteredUnit(t *testing.T) {
	in := StepInput{Registered: laborAt(t, "PACK", "SIM1", 12, UnitLine), Location: "SIM1"}
	_, err := ComposeStepCapacity("PACK", in, mustWorkloadProfile(t, f64ptr(2.5), f64ptr(1)))
	if !errors.Is(err, ErrUnsupportedNormalizationUnit) {
		t.Fatalf("got %v, want ErrUnsupportedNormalizationUnit", err)
	}
}

func TestComposeStepCapacity_PropagatesMissingConversionFactor(t *testing.T) {
	// The derived candidate is in PACKAGE but the profile has no
	// packages_per_order, even though LABOR (UNIT) would normalize.
	in := fixtureAPack(t)
	in.Registered = laborAt(t, "PACK", "SIM1", 2500, UnitUnit)
	_, err := ComposeStepCapacity("PACK", in, mustWorkloadProfile(t, f64ptr(2.5), nil))
	if !errors.Is(err, ErrMissingConversionFactor) {
		t.Fatalf("got %v, want ErrMissingConversionFactor", err)
	}
}

// Tie-break is the documented rule: the earliest candidate wins (registered in
// registration order, then the derived STATION constraint). 5 x 180 = 900 ==
// LABOR 900.
func TestComposeStepCapacity_TieGoesToRegisteredConstraint(t *testing.T) {
	in := fixtureAPack(t)
	in.Registered = laborAt(t, "PACK", "SIM1", 900, UnitPackage)
	in.StationCount = 5
	got, err := ComposeStepCapacity("PACK", in, mustWorkloadProfile(t, nil, f64ptr(1)))
	if err != nil {
		t.Fatal(err)
	}
	if got.Binding != ConstraintLabor {
		t.Fatalf("binding = %s, want LABOR (earliest wins a tie)", got.Binding)
	}
}

// --- FIXTURE B: the full path ------------------------------------------------

func fixtureBInputs(t *testing.T, rebinUnitsPerHour float64) map[ProcessType]StepInput {
	return map[ProcessType]StepInput{
		"PICK":  {Registered: laborAt(t, "PICK", "SIM1", 8000, UnitUnit), Location: "SIM1"},
		"REBIN": {Registered: laborAt(t, "REBIN", "SIM1", rebinUnitsPerHour, UnitUnit), Location: "SIM1"},
		"PACK":  fixtureAPack(t),
	}
}

func TestComposeProcessPathCapacity_FixtureB_RebinBottleneckUnchanged(t *testing.T) {
	path := mustProcessPath(t, "pick-rebin-pack", "Pick-Rebin-Pack", []processpath.ProcessType{"PICK", "REBIN", "PACK"})
	profile := mustWorkloadProfile(t, f64ptr(2.5), f64ptr(1))

	got, err := ComposeProcessPathCapacity(path, fixtureBInputs(t, 2500), profile)
	if err != nil {
		t.Fatal(err)
	}
	assertSteps(t, got, 3200, 1000, 1800)
	if got.Rate.Quantity() != 1000 || got.BottleneckStep != "REBIN" || got.BottleneckConstraint != ConstraintLabor {
		t.Fatalf("got %v bottleneck %s bound by %s, want 1000 REBIN LABOR", got.Rate.Quantity(), got.BottleneckStep, got.BottleneckConstraint)
	}
}

func TestComposeProcessPathCapacity_FixtureB_RaisedRebinMakesPackStationTheBottleneck(t *testing.T) {
	path := mustProcessPath(t, "pick-rebin-pack", "Pick-Rebin-Pack", []processpath.ProcessType{"PICK", "REBIN", "PACK"})
	profile := mustWorkloadProfile(t, f64ptr(2.5), f64ptr(1))

	got, err := ComposeProcessPathCapacity(path, fixtureBInputs(t, 6000), profile)
	if err != nil {
		t.Fatal(err)
	}
	assertSteps(t, got, 3200, 2400, 1800)
	if got.Rate.Quantity() != 1800 || got.Rate.Unit() != UnitOrder || got.BottleneckStep != "PACK" || got.BottleneckConstraint != ConstraintStation {
		t.Fatalf("got %v %s bottleneck %s bound by %s, want 1800 ORDER PACK STATION",
			got.Rate.Quantity(), got.Rate.Unit(), got.BottleneckStep, got.BottleneckConstraint)
	}
	if got.Steps[2].Binding != ConstraintStation || got.Steps[0].Binding != ConstraintLabor {
		t.Fatalf("step bindings %s/%s/%s, want LABOR/LABOR/STATION", got.Steps[0].Binding, got.Steps[1].Binding, got.Steps[2].Binding)
	}
}

func TestComposeProcessPathCapacity_CollectsWarningsInPathOrder(t *testing.T) {
	path := mustProcessPath(t, "pack-sort", "Pack-Sort", []processpath.ProcessType{"PACK", "SORT"})
	pack := fixtureAPack(t)
	pack.Standard = nil
	sort := StepInput{Registered: laborAt(t, "SORT", "SIM1", 900, UnitUnit), Location: "SIM1", StationCount: 3}
	got, err := ComposeProcessPathCapacity(path, map[ProcessType]StepInput{"PACK": pack, "SORT": sort}, mustWorkloadProfile(t, f64ptr(2.5), f64ptr(1)))
	if err != nil {
		t.Fatal(err)
	}
	if len(got.Warnings) != 2 || !strings.Contains(got.Warnings[0], "for PACK at SIM1") || !strings.Contains(got.Warnings[1], "for SORT at SIM1") {
		t.Fatalf("warnings = %q, want PACK's then SORT's", got.Warnings)
	}
}

func TestComposeProcessPathCapacity_MissingStepNamesTheStep(t *testing.T) {
	path := mustProcessPath(t, "pick-rebin-pack", "Pick-Rebin-Pack", []processpath.ProcessType{"PICK", "REBIN", "PACK"})
	inputs := fixtureBInputs(t, 2500)
	delete(inputs, "REBIN")
	_, err := ComposeProcessPathCapacity(path, inputs, mustWorkloadProfile(t, f64ptr(2.5), f64ptr(1)))
	if !errors.Is(err, ErrMissingStepCapacity) || !strings.HasSuffix(err.Error(), ": REBIN") {
		t.Fatalf("got %v, want ErrMissingStepCapacity naming REBIN", err)
	}
}

func assertSteps(t *testing.T, got PathCapacityResult, want ...float64) {
	t.Helper()
	if len(got.Steps) != len(want) {
		t.Fatalf("got %d steps, want %d", len(got.Steps), len(want))
	}
	for i, w := range want {
		if q := got.Steps[i].Rate.Quantity(); q != w {
			t.Errorf("step %d (%s) normalized = %v, want %v", i, got.Steps[i].Step, q, w)
		}
	}
}
