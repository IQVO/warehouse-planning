package usecases

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/claudioed/warehouse-planning/internal/domain/capacityplan"
)

func strptr(s string) *string { return &s }

// A scenario label is stored with the plan; several labelled drafts of one
// (site, path, window) coexist (docs/adr/0024 decision 2).
func TestCreateCapacityPlan_StoresTheScenarioLabel(t *testing.T) {
	f := newPlanFixture(t)
	ids := []string{"plan-a", "plan-b", "plan-c"}
	f.create.NewID = func() string { id := ids[0]; ids = ids[1:]; return id }
	for _, label := range []string{"baseline", "+15% volume"} {
		cmd := planCommand(9000)
		cmd.Scenario = strptr(label)
		plan, err := f.create.Handle(context.Background(), cmd)
		if err != nil || plan.Scenario() != label {
			t.Fatalf("%s: plan %v err %v", label, plan, err)
		}
	}
	for id, want := range map[string]string{"plan-a": "baseline", "plan-b": "+15% volume"} {
		stored, _ := f.plans.FindByID(context.Background(), id)
		if stored == nil || stored.Scenario() != want || stored.Status() != capacityplan.StatusDraft {
			t.Fatalf("%s: stored %+v, want the DRAFT scenario %q", id, stored, want)
		}
	}
	unlabelled, err := f.create.Handle(context.Background(), planCommand(1))
	if err != nil || unlabelled.Scenario() != "" {
		t.Fatalf("an unlabelled plan: %v, %v", unlabelled, err)
	}
}

func TestCreateCapacityPlan_RefusesAnEmptyOrTooLongScenario(t *testing.T) {
	for name, label := range map[string]string{"empty": "", "65 characters": strings.Repeat("x", 65)} {
		f := newPlanFixture(t)
		cmd := planCommand(10)
		cmd.Scenario = strptr(label)
		if _, err := f.create.Handle(context.Background(), cmd); !errors.Is(err, capacityplan.ErrInvalidScenario) {
			t.Errorf("%s: err = %v, want ErrInvalidScenario", name, err)
		}
		if len(f.outbox.Messages()) != 0 {
			t.Errorf("%s: a refused plan queued events", name)
		}
	}
}
