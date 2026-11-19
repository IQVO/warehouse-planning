package workloadforecast_test

import (
	"errors"
	"testing"

	"github.com/claudioed/warehouse-planning/internal/domain/workloadforecast"
)

func TestCanonicalPathID_TrimsAndUpperCases(t *testing.T) {
	for in, want := range map[string]string{"pick": "PICK", " PICK ": "PICK", "\tPick\n": "PICK", "PACK": "PACK", "   ": ""} {
		if got := workloadforecast.CanonicalPathID(in); got != want {
			t.Fatalf("CanonicalPathID(%q) = %q, want %q", in, got, want)
		}
	}
}

func TestNew_AnySpellingOfAPathIsStoredAndPublishedCanonical(t *testing.T) {
	for _, path := range []string{"pick", " PICK ", "Pick"} {
		f, ev, err := workloadforecast.New("S1", path, buckets(5), received)
		if err != nil {
			t.Fatal(err)
		}
		if f.PathID() != "PICK" || ev.PathID != "PICK" || ev.AggregateID() != "S1/PICK" {
			t.Fatalf("New(%q): stored %q, event %q key %q, want PICK and S1/PICK", path, f.PathID(), ev.PathID, ev.AggregateID())
		}
		r, err := workloadforecast.Rehydrate("S1", path, buckets(5), received, 2)
		if err != nil || r.PathID() != "PICK" {
			t.Fatalf("Rehydrate(%q) = %v, %v, want PICK", path, r, err)
		}
	}
}

func TestReceive_AnotherSpellingOfTheSamePathIsTheSameForecast(t *testing.T) {
	f, _, err := workloadforecast.New("S1", "PICK", buckets(5), received)
	if err != nil {
		t.Fatal(err)
	}
	ev, changed, err := f.Receive("S1", " pick ", buckets(7), received)
	if err != nil || !changed {
		t.Fatalf("Receive = changed %v, %v, want a change", changed, err)
	}
	if ev.PathID != "PICK" || ev.AggregateID() != "S1/PICK" || f.PathID() != "PICK" || f.Version() != 2 {
		t.Fatalf("event %q key %q, stored %q v%d, want PICK, S1/PICK, v2", ev.PathID, ev.AggregateID(), f.PathID(), f.Version())
	}
	if _, changed, err := f.Receive("S1", "Pick", buckets(7), received); err != nil || changed {
		t.Fatalf("an equal receipt under another spelling = changed %v, %v, want no change", changed, err)
	}
}

func TestPathBlankAfterTrimmingIsRefused(t *testing.T) {
	if _, _, err := workloadforecast.New("S1", "  ", buckets(1), received); !errors.Is(err, workloadforecast.ErrRequiredField) {
		t.Fatalf("New error = %v, want ErrRequiredField", err)
	}
	if _, err := workloadforecast.Rehydrate("S1", "\t", buckets(1), received, 1); !errors.Is(err, workloadforecast.ErrRequiredField) {
		t.Fatalf("Rehydrate error = %v, want ErrRequiredField", err)
	}
	f, _, err := workloadforecast.New("S1", "PICK", buckets(1), received)
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := f.Receive("S1", " ", buckets(1), received); !errors.Is(err, workloadforecast.ErrRequiredField) {
		t.Fatalf("Receive error = %v, want ErrRequiredField", err)
	}
}

func TestReceived_AggregateIDIsCanonical(t *testing.T) {
	if got := (workloadforecast.Received{SiteID: "S1", PathID: " pack "}).AggregateID(); got != "S1/PACK" {
		t.Fatalf("AggregateID = %q, want S1/PACK", got)
	}
}
