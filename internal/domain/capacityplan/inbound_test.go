package capacityplan

import (
	"errors"
	"testing"
	"time"
)

func inboundPlan(t *testing.T, in InboundDemand) (*CapacityPlan, error) {
	t.Helper()
	rate, bottleneck := phase2PathCapacity(t)
	return Create(CreateParams{
		ID: planID, WarehouseID: warehouseID, SiteID: siteID, Location: location,
		Window:         mustWindow(t, windowStart, windowEnd),
		ProcessPathID:  pathID,
		AssignedDemand: 8500,
		PathRate:       rate,
		BottleneckStep: bottleneck,
		Inbound:        in,
	}, createdAt)
}

func TestCreate_NoInboundIsTheZeroValue(t *testing.T) {
	plan, err := inboundPlan(t, InboundDemand{})
	if err != nil {
		t.Fatal(err)
	}
	if got := plan.Inbound(); got != (InboundDemand{}) {
		t.Fatalf("Inbound = %+v, want the zero value", got)
	}
	if plan.InboundAverageHeads() != 0 {
		t.Fatalf("average heads = %v, want 0 without inbound demand", plan.InboundAverageHeads())
	}
}

func TestCreate_KeepsTheInboundDemandNextToTheOrderDemand(t *testing.T) {
	in := InboundDemand{Appointments: 3, LabourMinutes: 240}
	plan, err := inboundPlan(t, in)
	if err != nil {
		t.Fatal(err)
	}
	if plan.Inbound() != in {
		t.Fatalf("Inbound = %+v, want %+v", plan.Inbound(), in)
	}
	if plan.AssignedDemand() != 8500 {
		t.Fatalf("inbound minutes leaked into the ORDER demand: %v", plan.AssignedDemand())
	}
	// 240 labour minutes over the plan window.
	want := 240 / plan.Window().Duration().Minutes()
	if got := plan.InboundAverageHeads(); got != want {
		t.Fatalf("average heads = %v, want %v", got, want)
	}
}

func TestCreate_StandardMissingIsKept(t *testing.T) {
	plan, err := inboundPlan(t, InboundDemand{Appointments: 2, StandardMissing: true})
	if err != nil {
		t.Fatal(err)
	}
	if got := plan.Inbound(); !got.StandardMissing || got.LabourMinutes != 0 || got.Appointments != 2 {
		t.Fatalf("Inbound = %+v", got)
	}
}

func TestCreate_RejectsNegativeInbound(t *testing.T) {
	for name, in := range map[string]InboundDemand{
		"negative appointments": {Appointments: -1},
		"negative minutes":      {LabourMinutes: -0.5},
	} {
		if _, err := inboundPlan(t, in); !errors.Is(err, ErrNegativeInbound) {
			t.Errorf("%s: err = %v, want ErrNegativeInbound", name, err)
		}
	}
}

func TestPublish_KeepsTheInboundDemand(t *testing.T) {
	in := InboundDemand{Appointments: 1, LabourMinutes: 30}
	plan, err := inboundPlan(t, in)
	if err != nil {
		t.Fatal(err)
	}
	if err := plan.Publish(publishedAt); err != nil {
		t.Fatal(err)
	}
	if plan.Inbound() != in {
		t.Fatalf("Inbound after publish = %+v", plan.Inbound())
	}
}

func TestRehydrate_Inbound(t *testing.T) {
	base := RehydrateParams{
		ID: planID, WarehouseID: warehouseID, Location: location,
		Window:    mustWindow(t, windowStart, windowEnd),
		CreatedAt: createdAt.Add(time.Second), Status: StatusDraft,
	}
	if got := Rehydrate(base).Inbound(); got != (InboundDemand{}) {
		t.Fatalf("a plan stored before migration 0009 reads back as %+v", got)
	}
	base.Inbound = InboundDemand{Appointments: 4, LabourMinutes: 120, StandardMissing: true}
	if got := Rehydrate(base).Inbound(); got != base.Inbound {
		t.Fatalf("Inbound = %+v, want %+v", got, base.Inbound)
	}
}

func TestInboundAverageHeadsOfAnEmptyWindowIsZero(t *testing.T) {
	plan := Rehydrate(RehydrateParams{ID: planID, Inbound: InboundDemand{LabourMinutes: 60}})
	if got := plan.InboundAverageHeads(); got != 0 {
		t.Fatalf("average heads = %v, want 0 for a zero window (never Inf/NaN)", got)
	}
}
