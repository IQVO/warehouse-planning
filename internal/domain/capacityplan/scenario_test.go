package capacityplan

import (
	"errors"
	"strings"
	"testing"
)

func TestValidateScenario(t *testing.T) {
	for _, ok := range []string{"a", "open the second pack line", strings.Repeat("x", 64), strings.Repeat("é", 64), " "} {
		if err := ValidateScenario(ok); err != nil {
			t.Errorf("ValidateScenario(%q) = %v, want nil", ok, err)
		}
	}
	for _, bad := range []string{"", strings.Repeat("x", 65), strings.Repeat("é", 65)} {
		if err := ValidateScenario(bad); !errors.Is(err, ErrInvalidScenario) {
			t.Errorf("ValidateScenario(%d chars) = %v, want ErrInvalidScenario", len([]rune(bad)), err)
		}
	}
}

func TestCreate_ScenarioIsOptionalAndBounded(t *testing.T) {
	rate, bottleneck := phase2PathCapacity(t)
	build := func(label string) (*CapacityPlan, error) {
		return Create(CreateParams{
			ID: planID, WarehouseID: warehouseID, SiteID: siteID, Location: location,
			Window: mustWindow(t, windowStart, windowEnd), ProcessPathID: pathID,
			AssignedDemand: 10, PathRate: rate, BottleneckStep: bottleneck, Scenario: label,
		}, createdAt)
	}
	if plan, err := build(""); err != nil || plan.Scenario() != "" {
		t.Fatalf("an unlabelled plan: %v, %v", plan, err)
	}
	if plan, err := build("+15% volume"); err != nil || plan.Scenario() != "+15% volume" {
		t.Fatalf("a labelled plan: %v, %v", plan, err)
	}
	if plan, err := build(strings.Repeat("x", 65)); !errors.Is(err, ErrInvalidScenario) || plan != nil {
		t.Fatalf("a 65-character label: %v, %v", plan, err)
	}
	if got := Rehydrate(RehydrateParams{ID: planID, Scenario: "baseline"}).Scenario(); got != "baseline" {
		t.Fatalf("Rehydrate lost the scenario: %q", got)
	}
}
