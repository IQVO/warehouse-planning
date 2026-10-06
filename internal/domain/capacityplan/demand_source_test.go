package capacityplan

import (
	"testing"
	"time"

	"github.com/claudioed/warehouse-planning/internal/domain/processcapacity"
)

func demandSourcePlan(t *testing.T, source DemandSource) *CapacityPlan {
	t.Helper()
	rate, bottleneck := phase2PathCapacity(t)
	plan, err := Create(CreateParams{
		ID: planID, WarehouseID: warehouseID, SiteID: siteID, Location: location,
		Window:         mustWindow(t, windowStart, windowEnd),
		ProcessPathID:  pathID,
		AssignedDemand: 8500,
		PathRate:       rate,
		BottleneckStep: bottleneck,
		DemandSource:   source,
	}, createdAt)
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	return plan
}

func TestCreate_DemandSourceDefaultsToRequest(t *testing.T) {
	if got := demandSourcePlan(t, "").DemandSource(); got != DemandSourceRequest {
		t.Fatalf("DemandSource = %q, want %q", got, DemandSourceRequest)
	}
}

func TestCreate_KeepsAnExplicitDemandSource(t *testing.T) {
	for _, src := range []DemandSource{DemandSourceRequest, DemandSourceOrders} {
		if got := demandSourcePlan(t, src).DemandSource(); got != src {
			t.Errorf("DemandSource = %q, want %q", got, src)
		}
	}
}

// Publishing must not alter provenance, and the published events must keep
// their payload (the source is informational, not part of any event).
func TestPublish_KeepsDemandSource(t *testing.T) {
	plan := demandSourcePlan(t, DemandSourceOrders)
	if err := plan.Publish(publishedAt); err != nil {
		t.Fatal(err)
	}
	if plan.DemandSource() != DemandSourceOrders {
		t.Fatalf("DemandSource after publish = %q", plan.DemandSource())
	}
}

func TestRehydrate_DemandSource(t *testing.T) {
	base := RehydrateParams{
		ID: planID, WarehouseID: warehouseID, Location: location,
		Window:    mustWindow(t, windowStart, windowEnd),
		CreatedAt: createdAt.Add(time.Second), Status: StatusDraft,
		BottleneckConstraint: processcapacity.ConstraintLabor,
	}
	if got := Rehydrate(base).DemandSource(); got != DemandSourceRequest {
		t.Fatalf("a plan stored before the column existed reads back as %q, want %q", got, DemandSourceRequest)
	}
	base.DemandSource = DemandSourceOrders
	if got := Rehydrate(base).DemandSource(); got != DemandSourceOrders {
		t.Fatalf("DemandSource = %q, want %q", got, DemandSourceOrders)
	}
}
