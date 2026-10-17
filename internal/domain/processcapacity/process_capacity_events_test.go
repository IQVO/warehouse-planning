package processcapacity

import (
	"testing"
	"time"
)

var (
	evStart = time.Date(2026, 10, 5, 8, 0, 0, 0, time.UTC)
	evEnd   = time.Date(2026, 10, 5, 16, 0, 0, 0, time.UTC)
	evAt    = time.Date(2026, 10, 4, 18, 45, 10, 0, time.UTC)
)

func evWindow(t *testing.T) CapacityWindow {
	t.Helper()
	w, err := NewCapacityWindow(evStart, evEnd)
	if err != nil {
		t.Fatal(err)
	}
	return w
}

func evRate(t *testing.T, q float64) CapacityRate {
	t.Helper()
	r, err := NewCapacityRate(q, UnitUnit, time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	return r
}

// The first constraint ever registered brings the aggregate into being and
// raises ProcessCapacityRegistered -- once, with the native constraint and
// its rate.
func TestFirstConstraintRaisesProcessCapacityRegistered(t *testing.T) {
	pc := NewProcessCapacity("PICK", "SIM1", evWindow(t))
	if err := pc.AddConstraint(ConstraintLabor, evRate(t, 4000), evAt); err != nil {
		t.Fatal(err)
	}

	events := pc.PullEvents()
	if len(events) != 1 {
		t.Fatalf("want 1 event, got %d", len(events))
	}
	reg, ok := events[0].(ProcessCapacityRegistered)
	if !ok {
		t.Fatalf("want ProcessCapacityRegistered, got %T", events[0])
	}
	if reg.EventName() != "ProcessCapacityRegistered" {
		t.Errorf("EventName = %q", reg.EventName())
	}
	if reg.ConstraintType != ConstraintLabor || reg.Rate.Quantity() != 4000 {
		t.Errorf("registered constraint = %s %v", reg.ConstraintType, reg.Rate.Quantity())
	}
	if !reg.OccurredAt().Equal(evAt) {
		t.Errorf("OccurredAt = %v, want %v", reg.OccurredAt(), evAt)
	}
	want := "PICK/SIM1/2026-10-05T08:00:00Z/2026-10-05T16:00:00Z"
	if reg.AggregateID() != want {
		t.Errorf("AggregateID = %q, want %q", reg.AggregateID(), want)
	}
	if pc.ID() != want {
		t.Errorf("ID = %q, want %q", pc.ID(), want)
	}
}

// A registration that lowers the minimum raises ProcessCapacityChanged
// with the before and after.
func TestLowerMinimumRaisesProcessCapacityChanged(t *testing.T) {
	pc := NewProcessCapacity("PICK", "SIM1", evWindow(t))
	_ = pc.AddConstraint(ConstraintLabor, evRate(t, 4000), evAt)
	_ = pc.AddConstraint(ConstraintLocation, evRate(t, 3500), evAt)
	events := pc.PullEvents()
	if len(events) != 2 {
		t.Fatalf("want 2 events, got %d", len(events))
	}
	ch, ok := events[1].(ProcessCapacityChanged)
	if !ok {
		t.Fatalf("want ProcessCapacityChanged, got %T", events[1])
	}
	if ch.EventName() != "ProcessCapacityChanged" {
		t.Errorf("EventName = %q", ch.EventName())
	}
	if ch.ConstraintType != ConstraintLocation {
		t.Errorf("cause = %s, want LOCATION", ch.ConstraintType)
	}
	if ch.PreviousRate.Quantity() != 4000 || ch.PreviousBinding != ConstraintLabor {
		t.Errorf("previous = %v %s", ch.PreviousRate.Quantity(), ch.PreviousBinding)
	}
	if ch.NewRate.Quantity() != 3500 || ch.NewBinding != ConstraintLocation {
		t.Errorf("new = %v %s", ch.NewRate.Quantity(), ch.NewBinding)
	}
}

// A binding flip at an equal rate is still a change: which constraint
// produces the effective rate is part of the answer.
func TestBindingFlipRaisesProcessCapacityChanged(t *testing.T) {
	pc := NewProcessCapacity("PICK", "SIM1", evWindow(t))
	_ = pc.AddConstraint(ConstraintLabor, evRate(t, 4000), evAt)
	_ = pc.AddConstraint(ConstraintEquipment, evRate(t, 4000), evAt)
	_ = pc.AddConstraint(ConstraintLabor, evRate(t, 5000), evAt)
	events := pc.PullEvents()
	// Registered, then nothing for the tie (earliest keeps the binding),
	// then Changed: LABOR 5000 hands the binding to EQUIPMENT 4000 at the
	// same rate.
	if len(events) != 2 {
		t.Fatalf("want 2 events, got %d", len(events))
	}
	ch, ok := events[1].(ProcessCapacityChanged)
	if !ok {
		t.Fatalf("want ProcessCapacityChanged, got %T", events[1])
	}
	if ch.PreviousRate.Quantity() != 4000 || ch.PreviousBinding != ConstraintLabor {
		t.Errorf("previous = %v %s", ch.PreviousRate.Quantity(), ch.PreviousBinding)
	}
	if ch.NewRate.Quantity() != 4000 || ch.NewBinding != ConstraintEquipment {
		t.Errorf("new = %v %s", ch.NewRate.Quantity(), ch.NewBinding)
	}
}

// Re-registering the only constraint at a higher rate changes the
// effective rate and raises ProcessCapacityChanged.
func TestReregistrationRaisesProcessCapacityChanged(t *testing.T) {
	pc := NewProcessCapacity("PICK", "SIM1", evWindow(t))
	_ = pc.AddConstraint(ConstraintLabor, evRate(t, 4000), evAt)
	_ = pc.AddConstraint(ConstraintLabor, evRate(t, 8000), evAt)
	events := pc.PullEvents()
	if len(events) != 2 {
		t.Fatalf("want 2 events, got %d", len(events))
	}
	ch := events[1].(ProcessCapacityChanged)
	if ch.ConstraintType != ConstraintLabor || ch.NewRate.Quantity() != 8000 {
		t.Errorf("cause %s, new %v", ch.ConstraintType, ch.NewRate.Quantity())
	}
}

// A registration the effective rate does not notice raises nothing.
func TestNoChangeNoEvent(t *testing.T) {
	pc := NewProcessCapacity("PICK", "SIM1", evWindow(t))
	_ = pc.AddConstraint(ConstraintLabor, evRate(t, 4000), evAt)
	if events := pc.PullEvents(); len(events) != 1 {
		t.Fatalf("want 1 event after first registration, got %d", len(events))
	}
	_ = pc.AddConstraint(ConstraintEquipment, evRate(t, 5000), evAt) // non-binding
	_ = pc.AddConstraint(ConstraintEquipment, evRate(t, 5000), evAt) // same rate again
	if events := pc.PullEvents(); len(events) != 0 {
		t.Fatalf("want no events, got %d: %v", len(events), events)
	}
}

// PullEvents hands each event over exactly once.
func TestPullEventsClears(t *testing.T) {
	pc := NewProcessCapacity("PICK", "SIM1", evWindow(t))
	_ = pc.AddConstraint(ConstraintLabor, evRate(t, 4000), evAt)
	if len(pc.PullEvents()) != 1 {
		t.Fatal("first PullEvents must return the event")
	}
	if len(pc.PullEvents()) != 0 {
		t.Fatal("second PullEvents must return nothing")
	}
}

// A rejected registration (unit mismatch) records nothing.
func TestUnitMismatchRecordsNothing(t *testing.T) {
	pc := NewProcessCapacity("PICK", "SIM1", evWindow(t))
	_ = pc.AddConstraint(ConstraintLabor, evRate(t, 4000), evAt)
	pc.PullEvents()

	line, err := NewCapacityRate(50, UnitPackage, time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	if err := pc.AddConstraint(ConstraintEquipment, line, evAt); err == nil {
		t.Fatal("want ErrUnitMismatch")
	}
	if events := pc.PullEvents(); len(events) != 0 {
		t.Fatalf("a rejected registration recorded %d events", len(events))
	}
}

// Rehydrate rebuilds persisted state silently: no events, ever.
func TestRehydrateRecordsNoEvents(t *testing.T) {
	entries := []ConstraintEntry{
		{Type: ConstraintLabor, Rate: evRate(t, 4000)},
		{Type: ConstraintLocation, Rate: evRate(t, 3500)},
	}
	pc, err := Rehydrate("PICK", "SIM1", evWindow(t), entries)
	if err != nil {
		t.Fatal(err)
	}
	rate, binding, err := pc.EffectiveRate()
	if err != nil || rate.Quantity() != 3500 || binding != ConstraintLocation {
		t.Fatalf("rehydrated effective = %v %s (%v)", rate.Quantity(), binding, err)
	}
	if events := pc.PullEvents(); len(events) != 0 {
		t.Fatalf("Rehydrate recorded %d events", len(events))
	}
}

// Rehydrate rejects persisted state that violates the single-native-unit
// invariant (corrupt rows, not a state this aggregate can be in).
func TestRehydrateRejectsMixedUnits(t *testing.T) {
	pkg, err := NewCapacityRate(10, UnitPackage, time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	_, err = Rehydrate("PICK", "SIM1", evWindow(t), []ConstraintEntry{
		{Type: ConstraintLabor, Rate: evRate(t, 4000)},
		{Type: ConstraintEquipment, Rate: pkg},
	})
	if err != ErrUnitMismatch {
		t.Fatalf("want ErrUnitMismatch, got %v", err)
	}
}
