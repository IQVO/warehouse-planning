package labourplan_test

import (
	"errors"
	"math"
	"testing"
	"time"

	"github.com/claudioed/warehouse-planning/internal/domain/labourplan"
)

var (
	shiftStart = time.Date(2026, 11, 20, 6, 0, 0, 0, time.UTC)
	commitAt   = time.Date(2026, 11, 20, 5, 0, 0, 0, time.UTC)
)

func spec() labourplan.Spec {
	start := shiftStart
	return labourplan.Spec{
		SiteID: "SITE-GRU-01", ShiftID: "2026-11-20-A", PathID: "PICK",
		PlannedHeads: 10, RateUnitsPerHour: 60, Hours: 8, InstalledStations: 12,
		ShiftStart: &start, HeadsSource: labourplan.HeadsFromRequest,
	}
}

func TestNew_ValidPlanIsVersionOneWithEveryFieldOnItsEvent(t *testing.T) {
	p, ev, err := labourplan.New(spec(), commitAt)
	if err != nil {
		t.Fatal(err)
	}
	if p.Version() != 1 || p.PlannedThroughput() != 4800 {
		t.Fatalf("plan v%d throughput %v, want v1 and 4800", p.Version(), p.PlannedThroughput())
	}
	got := ev
	got.ShiftStart = nil
	want := labourplan.Committed{
		SiteID: "SITE-GRU-01", ShiftID: "2026-11-20-A", PathID: "PICK",
		PlannedHeads: 10, RateUnitsPerHour: 60, Hours: 8, InstalledStations: 12,
		HeadsSource: labourplan.HeadsFromRequest, Version: 1, At: commitAt,
	}
	if got != want || ev.ShiftStart == nil || !ev.ShiftStart.Equal(shiftStart) {
		t.Fatalf("event = %+v (shift start %v), want %+v", got, ev.ShiftStart, want)
	}
	if ev.EventName() != "LabourPlanCommitted" || ev.AggregateID() != "SITE-GRU-01/2026-11-20-A/PICK" || !ev.OccurredAt().Equal(commitAt) {
		t.Fatalf("event header = %s %s %v", ev.EventName(), ev.AggregateID(), ev.OccurredAt())
	}
}

func TestNew_RejectsEveryBrokenInvariant(t *testing.T) {
	cases := []struct {
		name   string
		change func(*labourplan.Spec)
		want   error
	}{
		{"blank site", func(s *labourplan.Spec) { s.SiteID = "" }, labourplan.ErrRequiredField},
		{"blank shift", func(s *labourplan.Spec) { s.ShiftID = "" }, labourplan.ErrRequiredField},
		{"blank path", func(s *labourplan.Spec) { s.PathID = "" }, labourplan.ErrRequiredField},
		{"negative heads", func(s *labourplan.Spec) { s.PlannedHeads = -1 }, labourplan.ErrInvalidCount},
		{"heads above the bound", func(s *labourplan.Spec) {
			s.PlannedHeads, s.InstalledStations = labourplan.MaxCount+1, labourplan.MaxCount+1
		}, labourplan.ErrInvalidCount},
		{"negative stations", func(s *labourplan.Spec) { s.PlannedHeads, s.InstalledStations = 0, -1 }, labourplan.ErrInvalidCount},
		{"stations above the bound", func(s *labourplan.Spec) { s.InstalledStations = labourplan.MaxCount + 1 }, labourplan.ErrInvalidCount},
		{"heads one above the stations", func(s *labourplan.Spec) { s.PlannedHeads = 13 }, labourplan.ErrHeadsExceedStations},
		{"zero rate", func(s *labourplan.Spec) { s.RateUnitsPerHour = 0 }, labourplan.ErrInvalidRate},
		{"negative rate", func(s *labourplan.Spec) { s.RateUnitsPerHour = -5 }, labourplan.ErrInvalidRate},
		{"NaN rate", func(s *labourplan.Spec) { s.RateUnitsPerHour = math.NaN() }, labourplan.ErrInvalidRate},
		{"zero hours", func(s *labourplan.Spec) { s.Hours = 0 }, labourplan.ErrInvalidHours},
		{"NaN hours", func(s *labourplan.Spec) { s.Hours = math.NaN() }, labourplan.ErrInvalidHours},
		{"overflowing throughput", func(s *labourplan.Spec) { s.RateUnitsPerHour = math.MaxFloat64 }, labourplan.ErrThroughputNotFinite},
		{"infinite rate and zero heads", func(s *labourplan.Spec) { s.RateUnitsPerHour, s.PlannedHeads = math.Inf(1), 0 }, labourplan.ErrThroughputNotFinite},
		{"unknown heads source", func(s *labourplan.Spec) { s.HeadsSource = "guess" }, labourplan.ErrUnknownHeadsSource},
		{"empty heads source", func(s *labourplan.Spec) { s.HeadsSource = "" }, labourplan.ErrUnknownHeadsSource},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			s := spec()
			c.change(&s)
			if _, _, err := labourplan.New(s, commitAt); !errors.Is(err, c.want) {
				t.Fatalf("New error = %v, want %v", err, c.want)
			}
			if _, err := labourplan.Rehydrate(s, 2); !errors.Is(err, c.want) {
				t.Fatalf("Rehydrate error = %v, want %v", err, c.want)
			}
		})
	}
}

func TestNew_AcceptsEveryBoundaryValue(t *testing.T) {
	cases := []struct {
		name   string
		change func(*labourplan.Spec)
	}{
		{"heads equal to stations", func(s *labourplan.Spec) { s.PlannedHeads = 12 }},
		{"zero heads and zero stations", func(s *labourplan.Spec) { s.PlannedHeads, s.InstalledStations = 0, 0 }},
		{"both at the bound", func(s *labourplan.Spec) {
			s.PlannedHeads, s.InstalledStations = labourplan.MaxCount, labourplan.MaxCount
		}},
		{"a tiny rate", func(s *labourplan.Spec) { s.RateUnitsPerHour = math.SmallestNonzeroFloat64 }},
		{"a tiny shift", func(s *labourplan.Spec) { s.Hours = math.SmallestNonzeroFloat64 }},
		{"heads from the forecast", func(s *labourplan.Spec) { s.HeadsSource = labourplan.HeadsFromForecast }},
		{"no shift start", func(s *labourplan.Spec) { s.ShiftStart = nil }},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			s := spec()
			c.change(&s)
			if _, _, err := labourplan.New(s, commitAt); err != nil {
				t.Fatalf("New: %v", err)
			}
		})
	}
}

func TestRehydrate_VersionMustBeAtLeastOne(t *testing.T) {
	p, err := labourplan.Rehydrate(spec(), 1)
	if err != nil || p.Version() != 1 {
		t.Fatalf("version 1 = %v, %v", p, err)
	}
	for _, v := range []int64{0, -1} {
		if _, err := labourplan.Rehydrate(spec(), v); !errors.Is(err, labourplan.ErrInvalidVersion) {
			t.Fatalf("version %d error = %v, want ErrInvalidVersion", v, err)
		}
	}
}

func TestPlan_DoesNotShareItsShiftStartWithTheCaller(t *testing.T) {
	s := spec()
	p, ev, err := labourplan.New(s, commitAt)
	if err != nil {
		t.Fatal(err)
	}
	*s.ShiftStart = shiftStart.Add(time.Hour)
	got := p.Spec()
	if !got.ShiftStart.Equal(shiftStart) || !ev.ShiftStart.Equal(shiftStart) {
		t.Fatalf("the plan follows the caller's pointer: plan %v event %v", got.ShiftStart, ev.ShiftStart)
	}
	*got.ShiftStart = shiftStart.Add(2 * time.Hour)
	if again := p.Spec(); !again.ShiftStart.Equal(shiftStart) {
		t.Fatalf("Spec() hands out the plan's own pointer: %v", again.ShiftStart)
	}
}

func TestRevise_AChangeReplacesThePlanAndBumpsTheVersion(t *testing.T) {
	p, err := labourplan.Rehydrate(spec(), 3)
	if err != nil {
		t.Fatal(err)
	}
	s := spec()
	s.PlannedHeads = 11
	later := commitAt.Add(time.Hour)
	ev, changed, err := p.Revise(s, later)
	if err != nil || !changed {
		t.Fatalf("Revise = changed %v, %v", changed, err)
	}
	if ev.Version != 4 || ev.PlannedHeads != 11 || !ev.At.Equal(later) || p.Version() != 4 || p.Spec().PlannedHeads != 11 {
		t.Fatalf("after Revise: event %+v, plan v%d heads %d", ev, p.Version(), p.Spec().PlannedHeads)
	}
}

func TestRevise_AnEqualSpecChangesNothing(t *testing.T) {
	p, err := labourplan.Rehydrate(spec(), 3)
	if err != nil {
		t.Fatal(err)
	}
	ev, changed, err := p.Revise(spec(), commitAt.Add(time.Hour))
	if err != nil || changed || ev != (labourplan.Committed{}) || p.Version() != 3 {
		t.Fatalf("an equal spec = changed %v, event %+v, v%d, %v", changed, ev, p.Version(), err)
	}
}

func TestRevise_EveryFieldIsPartOfTheComparison(t *testing.T) {
	cases := map[string]func(*labourplan.Spec){
		"heads":         func(s *labourplan.Spec) { s.PlannedHeads = 9 },
		"rate":          func(s *labourplan.Spec) { s.RateUnitsPerHour = 61 },
		"hours":         func(s *labourplan.Spec) { s.Hours = 7.5 },
		"stations":      func(s *labourplan.Spec) { s.InstalledStations = 13 },
		"source":        func(s *labourplan.Spec) { s.HeadsSource = labourplan.HeadsFromForecast },
		"start moved":   func(s *labourplan.Spec) { moved := shiftStart.Add(time.Minute); s.ShiftStart = &moved },
		"start dropped": func(s *labourplan.Spec) { s.ShiftStart = nil },
	}
	for name, change := range cases {
		t.Run(name, func(t *testing.T) {
			p, err := labourplan.Rehydrate(spec(), 1)
			if err != nil {
				t.Fatal(err)
			}
			s := spec()
			change(&s)
			if _, changed, err := p.Revise(s, commitAt); err != nil || !changed {
				t.Fatalf("changed %v, %v", changed, err)
			}
		})
	}
	t.Run("start added", func(t *testing.T) {
		s := spec()
		s.ShiftStart = nil
		p, err := labourplan.Rehydrate(s, 1)
		if err != nil {
			t.Fatal(err)
		}
		if _, changed, err := p.Revise(spec(), commitAt); err != nil || !changed {
			t.Fatalf("changed %v, %v", changed, err)
		}
	})
	t.Run("same start in another location is no change", func(t *testing.T) {
		p, err := labourplan.Rehydrate(spec(), 1)
		if err != nil {
			t.Fatal(err)
		}
		s := spec()
		local := shiftStart.In(time.FixedZone("BRT", -3*3600))
		s.ShiftStart = &local
		if _, changed, err := p.Revise(s, commitAt); err != nil || changed {
			t.Fatalf("changed %v, %v", changed, err)
		}
	})
}

func TestRevise_RejectsAnotherIdentityAndInvalidSpecs(t *testing.T) {
	for name, change := range map[string]func(*labourplan.Spec){
		"site":  func(s *labourplan.Spec) { s.SiteID = "SITE-2" },
		"shift": func(s *labourplan.Spec) { s.ShiftID = "B" },
		"path":  func(s *labourplan.Spec) { s.PathID = "PACK" },
	} {
		t.Run(name, func(t *testing.T) {
			p, err := labourplan.Rehydrate(spec(), 1)
			if err != nil {
				t.Fatal(err)
			}
			s := spec()
			change(&s)
			if _, _, err := p.Revise(s, commitAt); !errors.Is(err, labourplan.ErrIdentityMismatch) {
				t.Fatalf("error = %v, want ErrIdentityMismatch", err)
			}
		})
	}
	p, err := labourplan.Rehydrate(spec(), 1)
	if err != nil {
		t.Fatal(err)
	}
	s := spec()
	s.Hours = 0
	if _, _, err := p.Revise(s, commitAt); !errors.Is(err, labourplan.ErrInvalidHours) {
		t.Fatalf("invalid revision error = %v", err)
	}
	if p.Version() != 1 || p.Spec().Hours != 8 {
		t.Fatalf("a refused revision changed the plan: v%d hours %v", p.Version(), p.Spec().Hours)
	}
}

func TestShiftEnd(t *testing.T) {
	p, _, err := labourplan.New(spec(), commitAt)
	if err != nil {
		t.Fatal(err)
	}
	end, ok := p.ShiftEnd()
	if !ok || !end.Equal(shiftStart.Add(8*time.Hour)) {
		t.Fatalf("ShiftEnd = %v, %v", end, ok)
	}
	if got := labourplan.ShiftEnd(shiftStart, 7.5); !got.Equal(shiftStart.Add(7*time.Hour + 30*time.Minute)) {
		t.Fatalf("ShiftEnd(7.5h) = %v", got)
	}
	s := spec()
	s.ShiftStart = nil
	q, _, err := labourplan.New(s, commitAt)
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := q.ShiftEnd(); ok {
		t.Fatal("a plan without a shift start reported a shift end")
	}
}

func TestDeriveHeads(t *testing.T) {
	cases := []struct {
		name  string
		units int64
		rate  float64
		hours float64
		heads int
	}{
		{"rounds up", 1000, 60, 8, 3},
		{"exact multiple", 960, 60, 8, 2},
		{"zero units due is zero heads", 0, 60, 8, 0},
		{"the bound itself", labourplan.MaxCount, 1, 1, labourplan.MaxCount},
		{"fractional shift", 450, 60, 7.5, 1},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			heads, err := labourplan.DeriveHeads(c.units, c.rate, c.hours)
			if err != nil || heads != c.heads {
				t.Fatalf("DeriveHeads(%d, %v, %v) = %d, %v; want %d", c.units, c.rate, c.hours, heads, err, c.heads)
			}
		})
	}
}

func TestDeriveHeads_Refusals(t *testing.T) {
	cases := []struct {
		name  string
		units int64
		rate  float64
		hours float64
		want  error
	}{
		{"zero rate", 100, 0, 8, labourplan.ErrInvalidRate},
		{"NaN rate", 100, math.NaN(), 8, labourplan.ErrInvalidRate},
		{"infinite rate", 100, math.Inf(1), 8, labourplan.ErrInvalidRate},
		{"zero hours", 100, 60, 0, labourplan.ErrInvalidHours},
		{"NaN hours", 100, 60, math.NaN(), labourplan.ErrInvalidHours},
		{"infinite hours", 100, 60, math.Inf(1), labourplan.ErrInvalidHours},
		{"negative units", -1, 60, 8, labourplan.ErrInvalidCount},
		{"one head above the bound", labourplan.MaxCount + 1, 1, 1, labourplan.ErrInvalidCount},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if _, err := labourplan.DeriveHeads(c.units, c.rate, c.hours); !errors.Is(err, c.want) {
				t.Fatalf("error = %v, want %v", err, c.want)
			}
		})
	}
}
