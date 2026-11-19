package labourplan_test

import (
	"errors"
	"testing"
	"time"

	"github.com/claudioed/warehouse-planning/internal/domain/labourplan"
	"github.com/claudioed/warehouse-planning/internal/domain/workloadforecast"
)

func TestCanonicalPathID_IsTheWorkloadForecastForm(t *testing.T) {
	for _, in := range []string{"pick", " PICK ", "\tPack\n", "", "  "} {
		if got, want := labourplan.CanonicalPathID(in), workloadforecast.CanonicalPathID(in); got != want {
			t.Fatalf("CanonicalPathID(%q) = %q, want %q", in, got, want)
		}
	}
	if got := labourplan.CanonicalPathID(" pick "); got != "PICK" {
		t.Fatalf("CanonicalPathID(\" pick \") = %q, want PICK", got)
	}
}

func specFor(path string) labourplan.Spec {
	s := spec()
	s.PathID = path
	return s
}

func TestNew_AnySpellingOfAPathIsStoredAndPublishedCanonical(t *testing.T) {
	for _, path := range []string{"pick", " PICK ", "Pick"} {
		p, ev, err := labourplan.New(specFor(path), commitAt)
		if err != nil {
			t.Fatal(err)
		}
		if p.Spec().PathID != "PICK" || ev.PathID != "PICK" || ev.AggregateID() != "SITE-GRU-01/2026-11-20-A/PICK" {
			t.Fatalf("New(%q): stored %q, event %q key %q", path, p.Spec().PathID, ev.PathID, ev.AggregateID())
		}
		r, err := labourplan.Rehydrate(specFor(path), 2)
		if err != nil || r.Spec().PathID != "PICK" {
			t.Fatalf("Rehydrate(%q) = %v, %v, want PICK", path, r, err)
		}
	}
}

func TestRevise_AnotherSpellingOfTheSamePathIsTheSamePlan(t *testing.T) {
	p, _, err := labourplan.New(specFor("PICK"), commitAt)
	if err != nil {
		t.Fatal(err)
	}
	if _, changed, err := p.Revise(specFor(" pick "), commitAt); err != nil || changed {
		t.Fatalf("an equal spec under another spelling = changed %v, %v, want no change", changed, err)
	}
	s := specFor("Pick")
	s.PlannedHeads = 11
	ev, changed, err := p.Revise(s, commitAt)
	if err != nil || !changed {
		t.Fatalf("Revise = changed %v, %v, want a change", changed, err)
	}
	if ev.PathID != "PICK" || p.Spec().PathID != "PICK" || p.Version() != 2 {
		t.Fatalf("event %q, stored %q v%d, want PICK v2", ev.PathID, p.Spec().PathID, p.Version())
	}
}

func TestPathBlankAfterTrimmingIsRefused(t *testing.T) {
	if _, _, err := labourplan.New(specFor("  "), commitAt); !errors.Is(err, labourplan.ErrRequiredField) {
		t.Fatalf("New error = %v, want ErrRequiredField", err)
	}
	if _, err := labourplan.Rehydrate(specFor("\t"), 1); !errors.Is(err, labourplan.ErrRequiredField) {
		t.Fatalf("Rehydrate error = %v, want ErrRequiredField", err)
	}
	p, _, err := labourplan.New(spec(), commitAt)
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := p.Revise(specFor(" "), commitAt); !errors.Is(err, labourplan.ErrRequiredField) {
		t.Fatalf("Revise error = %v, want ErrRequiredField", err)
	}
}

func TestEventKeysAreCanonical(t *testing.T) {
	if got := (labourplan.Committed{SiteID: "S1", ShiftID: "A", PathID: " pick "}).AggregateID(); got != "S1/A/PICK" {
		t.Fatalf("Committed.AggregateID = %q, want S1/A/PICK", got)
	}
	if got := (labourplan.DriftDetected{SiteID: "S1", ShiftID: "A", PathID: "pack"}).AggregateID(); got != "S1/A/PACK" {
		t.Fatalf("DriftDetected.AggregateID = %q, want S1/A/PACK", got)
	}
	ev := labourplan.CompareHeads(4, 6).Event("S1", "A", " pick ", commitAt)
	if ev.PathID != "PICK" || ev.AggregateID() != "S1/A/PICK" {
		t.Fatalf("Drift.Event path %q key %q, want PICK and S1/A/PICK", ev.PathID, ev.AggregateID())
	}
}

func TestNewCommitment_HoldsTheCanonicalPath(t *testing.T) {
	observed := time.Date(2026, 11, 20, 5, 45, 0, 0, time.UTC)
	for _, path := range []string{"pick", " PICK ", "Pick"} {
		c, err := labourplan.NewCommitment("S1", "A", path, 7, observed)
		if err != nil {
			t.Fatal(err)
		}
		want := labourplan.Commitment{SiteID: "S1", ShiftID: "A", PathID: "PICK", Heads: 7, ObservedAt: observed}
		if c != want {
			t.Fatalf("NewCommitment(%q) = %+v, want %+v", path, c, want)
		}
	}
}

func TestNewCommitment_AcceptsTheHeadsBounds(t *testing.T) {
	for _, heads := range []int{0, labourplan.MaxCount} {
		c, err := labourplan.NewCommitment("S1", "A", "PICK", heads, commitAt)
		if err != nil || c.Heads != heads {
			t.Fatalf("NewCommitment(heads %d) = %+v, %v", heads, c, err)
		}
	}
}

func TestNewCommitment_Refusals(t *testing.T) {
	cases := []struct {
		name              string
		site, shift, path string
		heads             int
		want              error
	}{
		{"blank site", " ", "A", "PICK", 1, labourplan.ErrRequiredField},
		{"blank shift", "S1", "", "PICK", 1, labourplan.ErrRequiredField},
		{"path blank after trimming", "S1", "A", "  ", 1, labourplan.ErrRequiredField},
		{"negative heads", "S1", "A", "PICK", -1, labourplan.ErrInvalidCount},
		{"heads above MaxCount", "S1", "A", "PICK", labourplan.MaxCount + 1, labourplan.ErrInvalidCount},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got, err := labourplan.NewCommitment(c.site, c.shift, c.path, c.heads, commitAt); !errors.Is(err, c.want) || got != (labourplan.Commitment{}) {
				t.Fatalf("NewCommitment = %+v, %v, want %v", got, err, c.want)
			}
		})
	}
}
