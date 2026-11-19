package usecases

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/claudioed/warehouse-planning/internal/adapters/outbound/memory"
	"github.com/claudioed/warehouse-planning/internal/application/ports"
	"github.com/claudioed/warehouse-planning/internal/domain/capacityplan"
	"github.com/claudioed/warehouse-planning/internal/domain/processcapacity"
)

// comparedPlans stores plans of SIM1 and the plan window, one of another
// site and one of another window, and returns the use case over them.
func comparedPlans(t *testing.T) *CompareCapacityPlans {
	t.Helper()
	repo := memory.NewCapacityPlanRepo()
	other, err := processcapacity.NewCapacityWindow(planWindowStart, planWindowEnd.Add(-time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	same, _ := processcapacity.NewCapacityWindow(planWindowStart, planWindowEnd)
	for _, p := range []capacityplan.RehydrateParams{
		{ID: "a", SiteID: "SIM1", Window: same, Scenario: "baseline", AssignedDemand: 9000, Shortage: 1000, Status: capacityplan.StatusDraft},
		{ID: "b", SiteID: "SIM1", Window: same, Scenario: "+15%", AssignedDemand: 10350, Shortage: 2350, Status: capacityplan.StatusPublished},
		{ID: "c", SiteID: "SIM2", Window: same},
		{ID: "d", SiteID: "SIM1", Window: other},
	} {
		if err := repo.Save(context.Background(), capacityplan.Rehydrate(p)); err != nil {
			t.Fatal(err)
		}
	}
	return &CompareCapacityPlans{Plans: repo}
}

func TestCompareCapacityPlans_ReturnsTheStoredPlansInOrder(t *testing.T) {
	plans, err := comparedPlans(t).Handle(context.Background(), []string{"b", "a"})
	if err != nil {
		t.Fatal(err)
	}
	if len(plans) != 2 || plans[0].ID() != "b" || plans[1].ID() != "a" {
		t.Fatalf("plans = %v, want b then a", plans)
	}
	if plans[0].Shortage() != 2350 || plans[0].Scenario() != "+15%" || plans[1].AssignedDemand() != 9000 {
		t.Fatalf("the stored figures changed: %+v / %+v", plans[0], plans[1])
	}
}

func TestCompareCapacityPlans_Errors(t *testing.T) {
	uc := comparedPlans(t)
	for name, c := range map[string]struct {
		ids  []string
		want error
	}{
		"one id":               {[]string{"a"}, capacityplan.ErrInvalidComparison},
		"six ids":              {[]string{"a", "b", "c", "d", "e", "f"}, capacityplan.ErrInvalidComparison},
		"a repeated id":        {[]string{"a", "a"}, capacityplan.ErrInvalidComparison},
		"an unknown id":        {[]string{"a", "nope"}, ErrCapacityPlanNotFound},
		"another site":         {[]string{"a", "c"}, capacityplan.ErrComparisonSiteMismatch},
		"another window":       {[]string{"a", "b", "d"}, capacityplan.ErrComparisonWindowMismatch},
		"unknown before scope": {[]string{"c", "nope"}, ErrCapacityPlanNotFound},
	} {
		if _, err := uc.Handle(context.Background(), c.ids); !errors.Is(err, c.want) {
			t.Errorf("%s: err = %v, want %v", name, err, c.want)
		}
	}
	boom := errors.New("boom")
	failing := &CompareCapacityPlans{Plans: unreadablePlans{CapacityPlanRepository: memory.NewCapacityPlanRepo(), err: boom}}
	if _, err := failing.Handle(context.Background(), []string{"a", "b"}); !errors.Is(err, boom) {
		t.Errorf("a store failure: err = %v, want it propagated", err)
	}
}

// unreadablePlans fails every FindByID.
type unreadablePlans struct {
	ports.CapacityPlanRepository
	err error
}

func (u unreadablePlans) FindByID(context.Context, string) (*capacityplan.CapacityPlan, error) {
	return nil, u.err
}
