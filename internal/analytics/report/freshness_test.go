package report

import (
	"testing"
	"time"
)

func TestComputeFreshness_NothingAppliedYetIsNilNotZero(t *testing.T) {
	if got := ComputeFreshness(nil, now); got.AsOf != nil || got.LagSeconds != nil {
		t.Fatalf("freshness = %+v, want both nil", got)
	}
}

func TestComputeFreshness_LagIsNowMinusTheNewestEventTime(t *testing.T) {
	asOf := now.Add(-95*time.Second - 500*time.Millisecond)
	got := ComputeFreshness(&asOf, now)
	if got.LagSeconds == nil || *got.LagSeconds != 95.5 {
		t.Fatalf("lag = %v, want 95.5", got.LagSeconds)
	}
	if got.AsOf == nil || !got.AsOf.Equal(asOf) {
		t.Fatalf("as_of = %v, want %v", got.AsOf, asOf)
	}
}

func TestComputeFreshness_ReportsAsOfInUTC(t *testing.T) {
	zone := time.FixedZone("BRT", -3*3600)
	asOf := now.Add(-time.Minute).In(zone)
	got := ComputeFreshness(&asOf, now)
	if got.AsOf.Location() != time.UTC {
		t.Fatalf("as_of location = %v, want UTC", got.AsOf.Location())
	}
}

func TestComputeFreshness_AClockBehindTheEventReadsZeroLag(t *testing.T) {
	asOf := now.Add(3 * time.Second)
	got := ComputeFreshness(&asOf, now)
	if got.LagSeconds == nil || *got.LagSeconds != 0 {
		t.Fatalf("lag = %v, want 0 (never negative)", got.LagSeconds)
	}
	same := now
	if got := ComputeFreshness(&same, now); *got.LagSeconds != 0 {
		t.Fatalf("lag at the same instant = %v", *got.LagSeconds)
	}
	justBefore := now.Add(-time.Second)
	if got := ComputeFreshness(&justBefore, now); *got.LagSeconds != 1 {
		t.Fatalf("lag one second behind = %v, want 1", *got.LagSeconds)
	}
}
