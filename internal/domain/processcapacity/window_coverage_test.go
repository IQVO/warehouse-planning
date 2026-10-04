package processcapacity

import (
	"errors"
	"testing"
	"time"
)

// Window coverage (docs/adr/0003): a constraint registered for window C applies
// to a planning window W when C.start <= W.start AND C.end >= W.end.

var coverBase = time.Date(2026, 10, 5, 8, 0, 0, 0, time.UTC)

func mustWindow(t *testing.T, start, end time.Time) CapacityWindow {
	t.Helper()
	w, err := NewCapacityWindow(start, end)
	if err != nil {
		t.Fatal(err)
	}
	return w
}

// hoursFromBase is the instant h hours after coverBase.
func hoursFromBase(h float64) time.Time {
	return coverBase.Add(time.Duration(h * float64(time.Hour)))
}

func TestCapacityWindow_Covers(t *testing.T) {
	requested := mustWindow(t, hoursFromBase(2), hoursFromBase(6))
	cases := []struct {
		name       string
		start, end time.Time
		want       bool
	}{
		{"equal window covers it", hoursFromBase(2), hoursFromBase(6), true},
		{"strictly wider on both sides", hoursFromBase(0), hoursFromBase(10), true},
		{"same start, later end", hoursFromBase(2), hoursFromBase(9), true},
		{"earlier start, same end", hoursFromBase(-3), hoursFromBase(6), true},
		{"start one second later does not", hoursFromBase(2).Add(time.Second), hoursFromBase(6), false},
		{"end one second earlier does not", hoursFromBase(2), hoursFromBase(6).Add(-time.Second), false},
		{"disjoint before", hoursFromBase(-5), hoursFromBase(-4), false},
		{"disjoint after", hoursFromBase(7), hoursFromBase(9), false},
		{"narrower inside", hoursFromBase(3), hoursFromBase(5), false},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got := mustWindow(t, c.start, c.end).Covers(requested)
			if got != c.want {
				t.Fatalf("[%v, %v).Covers([2h, 6h)) = %v, want %v", c.start, c.end, got, c.want)
			}
		})
	}
}

func pcAt(t *testing.T, startH, endH float64, constraints ...any) *ProcessCapacity {
	t.Helper()
	pc := NewProcessCapacity("PICK", "SIM1", mustWindow(t, hoursFromBase(startH), hoursFromBase(endH)))
	for i := 0; i < len(constraints); i += 2 {
		if err := pc.AddConstraint(constraints[i].(ConstraintType), constraints[i+1].(CapacityRate)); err != nil {
			t.Fatal(err)
		}
	}
	return pc
}

func TestSortNewestFirst_LaterStartThenNarrowerEnd(t *testing.T) {
	oldWide := pcAt(t, 0, 20)
	oldNarrow := pcAt(t, 0, 10)
	newWide := pcAt(t, 2, 20)
	newNarrow := pcAt(t, 2, 12)
	newest := pcAt(t, 4, 30)
	pcs := []*ProcessCapacity{oldWide, newWide, oldNarrow, newest, newNarrow}
	SortNewestFirst(pcs)
	want := []*ProcessCapacity{newest, newNarrow, newWide, oldNarrow, oldWide}
	for i := range want {
		if pcs[i] != want[i] {
			t.Fatalf("position %d: got window [%v, %v), want [%v, %v)", i,
				pcs[i].Window().Start(), pcs[i].Window().End(), want[i].Window().Start(), want[i].Window().End())
		}
	}
}

func TestSortNewestFirst_EmptyAndSingle(t *testing.T) {
	SortNewestFirst(nil)
	one := []*ProcessCapacity{pcAt(t, 0, 1)}
	SortNewestFirst(one)
	if len(one) != 1 {
		t.Fatal("single element changed")
	}
}

// coverProfile: 2.5 units per order, 1 package per order (design doc profile).
func coverProfile(t *testing.T) WorkloadProfile {
	return mustWorkloadProfile(t, f64ptr(2.5), f64ptr(1))
}

func perHour(t *testing.T, q float64, unit CapacityUnit) CapacityRate {
	return mustRate(t, q, unit, time.Hour)
}

func composeCovering(t *testing.T, covering ...*ProcessCapacity) (StepResult, error) {
	t.Helper()
	return ComposeStepCapacity("PICK", StepInput{Covering: covering, Location: "SIM1"}, coverProfile(t))
}

// Newest wins per type, whatever the order the covering aggregates arrive in.
func TestComposeStepCapacity_Covering_LaterStartWinsPerType(t *testing.T) {
	older := pcAt(t, 0, 40, ConstraintLabor, perHour(t, 100, UnitUnit))  // 40 ORDER/h if it won
	newer := pcAt(t, 8, 40, ConstraintLabor, perHour(t, 4000, UnitUnit)) // 1600 ORDER/h
	for name, in := range map[string][]*ProcessCapacity{"newest first": {newer, older}, "oldest first": {older, newer}} {
		t.Run(name, func(t *testing.T) {
			got, err := composeCovering(t, in...)
			if err != nil {
				t.Fatal(err)
			}
			if got.Rate.Quantity() != 1600 || got.Rate.Unit() != UnitOrder || got.Binding != ConstraintLabor {
				t.Fatalf("got %v %s bound by %s, want 1600 ORDER bound by LABOR (the older, lower LABOR is shadowed)", got.Rate.Quantity(), got.Rate.Unit(), got.Binding)
			}
		})
	}
}

// Same start: the narrower window (earlier end) takes precedence.
func TestComposeStepCapacity_Covering_SameStartNarrowerEndWins(t *testing.T) {
	wide := pcAt(t, 0, 40, ConstraintLabor, perHour(t, 100, UnitUnit))
	narrow := pcAt(t, 0, 12, ConstraintLabor, perHour(t, 4000, UnitUnit))
	for name, in := range map[string][]*ProcessCapacity{"narrow first": {narrow, wide}, "wide first": {wide, narrow}} {
		t.Run(name, func(t *testing.T) {
			got, err := composeCovering(t, in...)
			if err != nil {
				t.Fatal(err)
			}
			if got.Rate.Quantity() != 1600 {
				t.Fatalf("got %v, want 1600 (the narrower window's LABOR)", got.Rate.Quantity())
			}
		})
	}
}

// Shadowing is per constraint type: an older LABOR and a newer EQUIPMENT both
// apply and the minimum is taken (in either direction).
func TestComposeStepCapacity_Covering_ShadowingIsPerTypeOnly(t *testing.T) {
	t.Run("older LABOR binds below newer EQUIPMENT", func(t *testing.T) {
		older := pcAt(t, 0, 40, ConstraintLabor, perHour(t, 1000, UnitUnit)) // 400 ORDER/h
		newer := pcAt(t, 8, 40, ConstraintEquipment, perHour(t, 5000, UnitUnit))
		got, err := composeCovering(t, newer, older)
		if err != nil {
			t.Fatal(err)
		}
		if got.Rate.Quantity() != 400 || got.Binding != ConstraintLabor {
			t.Fatalf("got %v bound by %s, want 400 bound by LABOR", got.Rate.Quantity(), got.Binding)
		}
	})
	t.Run("newer EQUIPMENT binds below older LABOR", func(t *testing.T) {
		older := pcAt(t, 0, 40, ConstraintLabor, perHour(t, 5000, UnitUnit)) // 2000 ORDER/h
		newer := pcAt(t, 8, 40, ConstraintEquipment, perHour(t, 1000, UnitUnit))
		got, err := composeCovering(t, older, newer)
		if err != nil {
			t.Fatal(err)
		}
		if got.Rate.Quantity() != 400 || got.Binding != ConstraintEquipment {
			t.Fatalf("got %v bound by %s, want 400 bound by EQUIPMENT", got.Rate.Quantity(), got.Binding)
		}
	})
	t.Run("an older type the newer aggregate lacks still applies while the shadowed type does not", func(t *testing.T) {
		older := pcAt(t, 0, 40,
			ConstraintLabor, perHour(t, 50, UnitUnit), // shadowed by newer LABOR
			ConstraintLocation, perHour(t, 2500, UnitUnit)) // 1000 ORDER/h, not shadowed
		newer := pcAt(t, 8, 40, ConstraintLabor, perHour(t, 4000, UnitUnit)) // 1600 ORDER/h
		got, err := composeCovering(t, newer, older)
		if err != nil {
			t.Fatal(err)
		}
		if got.Rate.Quantity() != 1000 || got.Binding != ConstraintLocation {
			t.Fatalf("got %v bound by %s, want 1000 bound by LOCATION", got.Rate.Quantity(), got.Binding)
		}
	})
}

// Candidates normalize to ORDER/hour BEFORE the min: the covering aggregates'
// native units may differ (the stored aggregate's invariant is untouched).
func TestComposeStepCapacity_Covering_MixedNativeUnitsAcrossAggregates(t *testing.T) {
	older := pcAt(t, 0, 40, ConstraintLabor, perHour(t, 2500, UnitUnit))        // 1000 ORDER/h
	newer := pcAt(t, 8, 40, ConstraintEquipment, perHour(t, 1800, UnitPackage)) // 1800 ORDER/h
	got, err := composeCovering(t, older, newer)
	if err != nil {
		t.Fatal(err)
	}
	// Raw, 1800 < 2500 would wrongly pick EQUIPMENT; normalized, LABOR binds.
	if got.Rate.Quantity() != 1000 || got.Binding != ConstraintLabor || got.Rate.Unit() != UnitOrder {
		t.Fatalf("got %v %s bound by %s, want 1000 ORDER bound by LABOR", got.Rate.Quantity(), got.Rate.Unit(), got.Binding)
	}
}

// With equal normalized rates the earliest candidate wins, and candidates are
// ordered newest aggregate first.
func TestComposeStepCapacity_Covering_TieGoesToTheNewestAggregate(t *testing.T) {
	older := pcAt(t, 0, 40, ConstraintLabor, perHour(t, 1000, UnitOrder))
	newer := pcAt(t, 8, 40, ConstraintEquipment, perHour(t, 1000, UnitOrder))
	got, err := composeCovering(t, older, newer)
	if err != nil {
		t.Fatal(err)
	}
	if got.Binding != ConstraintEquipment {
		t.Fatalf("binding = %s, want EQUIPMENT (newest aggregate first)", got.Binding)
	}
}

// Registered is the one-aggregate shorthand and composes with Covering; the
// derived STATION constraint still participates.
func TestComposeStepCapacity_Covering_RegisteredAndStationStillParticipate(t *testing.T) {
	registered := pcAt(t, 8, 12, ConstraintEquipment, perHour(t, 5000, UnitUnit)) // 2000 ORDER/h
	covering := pcAt(t, 0, 40, ConstraintLabor, perHour(t, 5000, UnitUnit))       // 2000 ORDER/h, other type
	got, err := ComposeStepCapacity("PACK", StepInput{
		Registered: registered, Covering: []*ProcessCapacity{covering}, Location: "SIM1",
		StationCount: 10, Standard: mustStandard(t, "SIM1", "PACK", 180, UnitPackage), // 1800 PACKAGE/h = 1800 ORDER/h
	}, coverProfile(t))
	if err != nil {
		t.Fatal(err)
	}
	if got.Rate.Quantity() != 1800 || got.Binding != ConstraintStation {
		t.Fatalf("got %v bound by %s, want 1800 bound by STATION", got.Rate.Quantity(), got.Binding)
	}
}

func TestComposeStepCapacity_Covering_SameTypeInRegisteredAndCoveringIsShadowed(t *testing.T) {
	registered := pcAt(t, 8, 12, ConstraintLabor, perHour(t, 4000, UnitUnit)) // later start wins: 1600
	covering := pcAt(t, 0, 40, ConstraintLabor, perHour(t, 100, UnitUnit))
	got, err := ComposeStepCapacity("PICK", StepInput{Registered: registered, Covering: []*ProcessCapacity{covering}}, coverProfile(t))
	if err != nil {
		t.Fatal(err)
	}
	if got.Rate.Quantity() != 1600 {
		t.Fatalf("got %v, want 1600", got.Rate.Quantity())
	}
}

func TestComposeStepCapacity_Covering_NoneAndNoStationIsMissing(t *testing.T) {
	_, err := composeCovering(t)
	if !errors.Is(err, ErrMissingStepCapacity) {
		t.Fatalf("got %v, want ErrMissingStepCapacity", err)
	}
}

// ComposeStepCapacity must not reorder the caller's slice (the repository's
// result is shared with nothing, but mutating inputs is a trap).
func TestComposeStepCapacity_Covering_DoesNotReorderTheInput(t *testing.T) {
	older := pcAt(t, 0, 40, ConstraintLabor, perHour(t, 100, UnitUnit))
	newer := pcAt(t, 8, 40, ConstraintLabor, perHour(t, 4000, UnitUnit))
	in := []*ProcessCapacity{older, newer}
	if _, err := composeCovering(t, in...); err != nil {
		t.Fatal(err)
	}
	if in[0] != older || in[1] != newer {
		t.Fatal("input slice was reordered")
	}
}
