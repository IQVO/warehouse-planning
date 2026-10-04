package usecases

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/claudioed/warehouse-planning/internal/adapters/outbound/memory"
	"github.com/claudioed/warehouse-planning/internal/domain/capacityplan"
	"github.com/claudioed/warehouse-planning/internal/domain/processcapacity"
	"github.com/claudioed/warehouse-planning/internal/domain/processpath"
)

// recordingPlanLister records what the use case asked the port for and
// answers with canned values.
type recordingPlanLister struct {
	location string
	limit    int
	calls    int
	plans    []*capacityplan.CapacityPlan
	err      error
}

func (r *recordingPlanLister) ListRecent(_ context.Context, location string, limit int) ([]*capacityplan.CapacityPlan, error) {
	r.calls++
	r.location, r.limit = location, limit
	return r.plans, r.err
}

func TestListCapacityPlans_AppliesDefaultAndCapToTheLimit(t *testing.T) {
	cases := []struct {
		name  string
		given int
		want  int
	}{
		{"zero means the default", 0, 20},
		{"negative means the default", -3, 20},
		{"one is honoured", 1, 1},
		{"a stated limit is honoured", 37, 37},
		{"the cap itself is honoured", 100, 100},
		{"above the cap is capped", 101, 100},
		{"far above the cap is capped", 100000, 100},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			lister := &recordingPlanLister{}
			uc := &ListCapacityPlans{Plans: lister}
			if _, err := uc.Handle(context.Background(), "SIM1", tc.given); err != nil {
				t.Fatal(err)
			}
			if lister.limit != tc.want || lister.location != "SIM1" || lister.calls != 1 {
				t.Errorf("port saw limit=%d location=%q calls=%d; want limit=%d location=SIM1 calls=1",
					lister.limit, lister.location, lister.calls, tc.want)
			}
		})
	}
	if DefaultCapacityPlanListLimit != 20 || MaxCapacityPlanListLimit != 100 {
		t.Errorf("default/max = %d/%d, contract says 20/100", DefaultCapacityPlanListLimit, MaxCapacityPlanListLimit)
	}
}

func TestListCapacityPlans_NeverReturnsNilAndPropagatesErrors(t *testing.T) {
	got, err := (&ListCapacityPlans{Plans: &recordingPlanLister{plans: nil}}).Handle(context.Background(), "", 0)
	if err != nil || got == nil || len(got) != 0 {
		t.Errorf("nil from the port => %#v, %v; want empty non-nil slice", got, err)
	}
	boom := errors.New("boom")
	if _, err := (&ListCapacityPlans{Plans: &recordingPlanLister{err: boom}}).Handle(context.Background(), "", 0); !errors.Is(err, boom) {
		t.Errorf("err = %v, want boom", err)
	}
}

func TestListCapacityPlans_ReturnsStoredPlansNewestFirst(t *testing.T) {
	plans := memory.NewCapacityPlanRepo()
	window, _ := processcapacity.NewCapacityWindow(planWindowStart, planWindowEnd)
	rate, _ := processcapacity.NewCapacityRate(1000, processcapacity.UnitOrder, time.Hour)
	for i := 0; i < 3; i++ {
		plan, err := capacityplan.Create(capacityplan.CreateParams{
			ID: fmt.Sprintf("plan-%d", i), WarehouseID: "WH-1", Location: "SIM1", Window: window,
			ProcessPathID: "p", AssignedDemand: 10, PathRate: rate, BottleneckStep: "PACK",
		}, planCreatedAt.Add(time.Duration(i)*time.Minute))
		if err != nil {
			t.Fatal(err)
		}
		if err := plans.Save(context.Background(), plan); err != nil {
			t.Fatal(err)
		}
	}
	got, err := (&ListCapacityPlans{Plans: plans}).Handle(context.Background(), "SIM1", 2)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 2 || got[0].ID() != "plan-2" || got[1].ID() != "plan-1" {
		t.Errorf("got %d plans starting %v, want plan-2 then plan-1", len(got), got)
	}
}

type fakePathLister struct {
	paths []processpath.ProcessPath
	err   error
}

func (f fakePathLister) List(context.Context) ([]processpath.ProcessPath, error) {
	return f.paths, f.err
}

func TestListProcessPaths(t *testing.T) {
	got, err := (&ListProcessPaths{Paths: fakePathLister{}}).Handle(context.Background())
	if err != nil || got == nil || len(got) != 0 {
		t.Errorf("nil from the port => %#v, %v; want empty non-nil slice", got, err)
	}
	boom := errors.New("boom")
	if _, err := (&ListProcessPaths{Paths: fakePathLister{err: boom}}).Handle(context.Background()); !errors.Is(err, boom) {
		t.Errorf("err = %v, want boom", err)
	}

	repo := memory.NewProcessPathRepo()
	path, _ := processpath.NewProcessPath("pick-rebin-pack", "Pick-Rebin-Pack", []processpath.ProcessType{"PICK", "REBIN", "PACK"})
	if err := repo.Save(context.Background(), path); err != nil {
		t.Fatal(err)
	}
	got, err = (&ListProcessPaths{Paths: repo}).Handle(context.Background())
	if err != nil || len(got) != 1 || got[0].ID() != "pick-rebin-pack" {
		t.Errorf("got %v, %v; want the registered path", got, err)
	}
}
