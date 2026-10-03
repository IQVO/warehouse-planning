package processcapacity

import (
	"errors"
	"testing"
	"time"
)

// pickZoneAWindow builds the exact window used by the design doc's worked
// example: 2026-10-05T08:00:00Z..09:00:00Z.
func pickZoneAWindow(t *testing.T) CapacityWindow {
	t.Helper()
	start := time.Date(2026, 10, 5, 8, 0, 0, 0, time.UTC)
	end := time.Date(2026, 10, 5, 9, 0, 0, 0, time.UTC)
	window, err := NewCapacityWindow(start, end)
	if err != nil {
		t.Fatalf("unexpected error building the fixture window: %v", err)
	}
	return window
}

func mustRate(t *testing.T, quantity float64, unit CapacityUnit, period time.Duration) CapacityRate {
	t.Helper()
	rate, err := NewCapacityRate(quantity, unit, period)
	if err != nil {
		t.Fatalf("unexpected error building fixture rate: %v", err)
	}
	return rate
}

// TestProcessCapacity_WorkedExample_PickZoneA reproduces, byte-for-byte, the
// design doc's PICK / PICK-ZONE-A worked example: LABOR=4000, LOCATION=3500,
// EQUIPMENT=5000, CONVEYOR=3800 (all UNIT/HOUR) over the 08:00-09:00Z
// window must yield an effective rate of 3500 UNIT/HOUR with LOCATION
// binding. Kept forever as a regression fixture -- do not change the
// numbers below without updating the design doc too.
func TestProcessCapacity_WorkedExample_PickZoneA(t *testing.T) {
	window := pickZoneAWindow(t)
	pc := NewProcessCapacity("PICK", "PICK-ZONE-A", window)

	if err := pc.AddConstraint(ConstraintLabor, mustRate(t, 4000, UnitUnit, time.Hour)); err != nil {
		t.Fatalf("unexpected error adding LABOR: %v", err)
	}
	if err := pc.AddConstraint(ConstraintLocation, mustRate(t, 3500, UnitUnit, time.Hour)); err != nil {
		t.Fatalf("unexpected error adding LOCATION: %v", err)
	}
	if err := pc.AddConstraint(ConstraintEquipment, mustRate(t, 5000, UnitUnit, time.Hour)); err != nil {
		t.Fatalf("unexpected error adding EQUIPMENT: %v", err)
	}
	if err := pc.AddConstraint(ConstraintConveyor, mustRate(t, 3800, UnitUnit, time.Hour)); err != nil {
		t.Fatalf("unexpected error adding CONVEYOR: %v", err)
	}

	effective, binding, err := pc.EffectiveRate()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if binding != ConstraintLocation {
		t.Fatalf("expected LOCATION to be binding, got %v", binding)
	}
	if effective.Quantity() != 3500 || effective.Unit() != UnitUnit || effective.Period() != time.Hour {
		t.Fatalf("expected 3500 UNIT/HOUR, got %v %v/%v", effective.Quantity(), effective.Unit(), effective.Period())
	}
}

func TestProcessCapacity_EffectiveRate_ErrorsWithZeroConstraints(t *testing.T) {
	pc := NewProcessCapacity("PICK", "PICK-ZONE-A", pickZoneAWindow(t))
	if _, _, err := pc.EffectiveRate(); !errors.Is(err, ErrNoConstraints) {
		t.Fatalf("expected ErrNoConstraints, got %v", err)
	}
}

func TestProcessCapacity_AddConstraint_RejectsMismatchedUnit(t *testing.T) {
	pc := NewProcessCapacity("PICK", "PICK-ZONE-A", pickZoneAWindow(t))
	if err := pc.AddConstraint(ConstraintLabor, mustRate(t, 100, UnitUnit, time.Hour)); err != nil {
		t.Fatalf("unexpected error adding first constraint: %v", err)
	}
	err := pc.AddConstraint(ConstraintEquipment, mustRate(t, 50, UnitPackage, time.Hour))
	if !errors.Is(err, ErrUnitMismatch) {
		t.Fatalf("expected ErrUnitMismatch, got %v", err)
	}
}

// TestProcessCapacity_AddConstraint_UpsertsExistingType proves
// re-registering a constraint type REPLACES the prior rate (the newer
// value wins) rather than adding a duplicate entry.
func TestProcessCapacity_AddConstraint_UpsertsExistingType(t *testing.T) {
	pc := NewProcessCapacity("PICK", "PICK-ZONE-A", pickZoneAWindow(t))

	if err := pc.AddConstraint(ConstraintLabor, mustRate(t, 4000, UnitUnit, time.Hour)); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if err := pc.AddConstraint(ConstraintLabor, mustRate(t, 3200, UnitUnit, time.Hour)); err != nil {
		t.Fatalf("unexpected error re-registering LABOR: %v", err)
	}

	if got := len(pc.Constraints()); got != 1 {
		t.Fatalf("expected exactly 1 constraint entry after upsert, got %d", got)
	}

	effective, binding, err := pc.EffectiveRate()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if binding != ConstraintLabor {
		t.Fatalf("expected LABOR to be binding, got %v", binding)
	}
	if effective.Quantity() != 3200 {
		t.Fatalf("expected the newer value (3200) to win, got %v", effective.Quantity())
	}
}

func TestProcessCapacity_Constraints_PreservesRegistrationOrder(t *testing.T) {
	pc := NewProcessCapacity("PICK", "PICK-ZONE-A", pickZoneAWindow(t))
	_ = pc.AddConstraint(ConstraintEquipment, mustRate(t, 5000, UnitUnit, time.Hour))
	_ = pc.AddConstraint(ConstraintLabor, mustRate(t, 4000, UnitUnit, time.Hour))

	entries := pc.Constraints()
	if len(entries) != 2 {
		t.Fatalf("expected 2 constraint entries, got %d", len(entries))
	}
	if entries[0].Type != ConstraintEquipment || entries[1].Type != ConstraintLabor {
		t.Fatalf("expected registration order [EQUIPMENT, LABOR], got [%v, %v]", entries[0].Type, entries[1].Type)
	}
}

func TestNewProcessCapacity_ExposesIdentity(t *testing.T) {
	window := pickZoneAWindow(t)
	pc := NewProcessCapacity("PICK", "PICK-ZONE-A", window)
	if pc.ProcessType() != "PICK" {
		t.Fatalf("expected process type PICK, got %v", pc.ProcessType())
	}
	if pc.Location() != "PICK-ZONE-A" {
		t.Fatalf("expected location PICK-ZONE-A, got %v", pc.Location())
	}
	if pc.Window() != window {
		t.Fatalf("expected window %v, got %v", window, pc.Window())
	}
	if pc.NativeUnit() != "" {
		t.Fatalf("expected an empty native unit before any constraint is added, got %v", pc.NativeUnit())
	}
}
