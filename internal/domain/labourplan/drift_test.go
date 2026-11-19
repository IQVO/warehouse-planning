package labourplan_test

import (
	"testing"
	"time"

	"github.com/claudioed/warehouse-planning/internal/domain/labourplan"
)

func TestCompareHeads_IsCommittedMinusPlanned(t *testing.T) {
	d := labourplan.CompareHeads(10, 7)
	if d.PlannedHeads != 10 || d.CommittedHeads != 7 || d.Heads != -3 || !d.Detected() {
		t.Fatalf("CompareHeads(10, 7) = %+v", d)
	}
	if up := labourplan.CompareHeads(4, 6); up.Heads != 2 {
		t.Fatalf("CompareHeads(4, 6).Heads = %d, want 2", up.Heads)
	}
	if same := labourplan.CompareHeads(5, 5); same.Heads != 0 || same.Detected() {
		t.Fatalf("CompareHeads(5, 5) = %+v, want no drift", same)
	}
}

func TestDrift_ShouldRaise(t *testing.T) {
	cases := []struct {
		name    string
		drift   labourplan.Drift
		last    int
		hasLast bool
		want    bool
	}{
		{"no drift, nothing recorded", labourplan.CompareHeads(5, 5), 0, false, false},
		{"no drift after a drift", labourplan.CompareHeads(5, 5), 2, true, false},
		{"first drift", labourplan.CompareHeads(5, 7), 0, false, true},
		{"the same drift again", labourplan.CompareHeads(5, 7), 2, true, false},
		{"a different drift", labourplan.CompareHeads(5, 8), 2, true, true},
		{"a drift that returns after clearing", labourplan.CompareHeads(5, 7), 0, true, true},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := c.drift.ShouldRaise(c.last, c.hasLast); got != c.want {
				t.Fatalf("ShouldRaise(%d, %v) = %v, want %v", c.last, c.hasLast, got, c.want)
			}
		})
	}
}

func TestDrift_Event(t *testing.T) {
	at := time.Date(2026, 11, 20, 9, 0, 0, 0, time.UTC)
	ev := labourplan.CompareHeads(10, 7).Event("S1", "A", "PICK", at)
	want := labourplan.DriftDetected{SiteID: "S1", ShiftID: "A", PathID: "PICK", PlannedHeads: 10, CommittedHeads: 7, DriftHeads: -3, At: at}
	if ev != want {
		t.Fatalf("Event = %+v, want %+v", ev, want)
	}
	if ev.EventName() != "LabourPlanDriftDetected" || ev.AggregateID() != "S1/A/PICK" || !ev.OccurredAt().Equal(at) {
		t.Fatalf("event header = %s %s %v", ev.EventName(), ev.AggregateID(), ev.OccurredAt())
	}
}
