package usecases

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/claudioed/warehouse-planning/internal/domain/pagecursor"

	"github.com/claudioed/warehouse-planning/internal/adapters/outbound/memory"
	"github.com/claudioed/warehouse-planning/internal/domain/capacityplan"
	"github.com/claudioed/warehouse-planning/internal/domain/processcapacity"
	"github.com/claudioed/warehouse-planning/internal/domain/processpath"
)

// recordingPlanLister records what the use case asked the port for and
// answers with canned values.
type recordingPlanLister struct {
	location string
	after    *pagecursor.Cursor
	limit    int
	calls    int
	plans    []*capacityplan.CapacityPlan
	err      error
}

func (r *recordingPlanLister) ListRecent(_ context.Context, location string, after *pagecursor.Cursor, limit int) ([]*capacityplan.CapacityPlan, error) {
	r.calls++
	r.location, r.after, r.limit = location, after, limit
	return r.plans, r.err
}

func TestListCapacityPlans_PassesLocationCursorAndLimitThrough(t *testing.T) {
	lister := &recordingPlanLister{}
	after := &pagecursor.Cursor{Sort: "2026-10-04T09:00:00Z", ID: "plan-1"}
	if _, err := (&ListCapacityPlans{Plans: lister}).Handle(context.Background(), "SIM1", after, 37); err != nil {
		t.Fatal(err)
	}
	if lister.limit != 37 || lister.location != "SIM1" || lister.calls != 1 || lister.after != after {
		t.Errorf("port saw limit=%d location=%q calls=%d after=%v", lister.limit, lister.location, lister.calls, lister.after)
	}
}

func TestListCapacityPlans_NeverReturnsNilAndPropagatesErrors(t *testing.T) {
	got, err := (&ListCapacityPlans{Plans: &recordingPlanLister{plans: nil}}).Handle(context.Background(), "", nil, 10)
	if err != nil || got == nil || len(got) != 0 {
		t.Errorf("nil from the port => %#v, %v; want empty non-nil slice", got, err)
	}
	boom := errors.New("boom")
	if _, err := (&ListCapacityPlans{Plans: &recordingPlanLister{err: boom}}).Handle(context.Background(), "", nil, 10); !errors.Is(err, boom) {
		t.Errorf("err = %v, want boom", err)
	}
}

func TestListCapacityPlans_ReturnsStoredPlansNewestFirst(t *testing.T) {
	plans := memory.NewCapacityPlanRepo()
	window, _ := processcapacity.NewCapacityWindow(planWindowStart, planWindowEnd)
	rate, _ := processcapacity.NewCapacityRate(1000, processcapacity.UnitOrder, time.Hour)
	for i := 0; i < 3; i++ {
		plan, err := capacityplan.Create(capacityplan.CreateParams{
			ID: fmt.Sprintf("plan-%d", i), WarehouseID: "WH-1", SiteID: "SIM1", Location: "SIM1", Window: window,
			ProcessPathID: "p", AssignedDemand: 10, PathRate: rate, BottleneckStep: "PACK",
		}, planCreatedAt.Add(time.Duration(i)*time.Minute))
		if err != nil {
			t.Fatal(err)
		}
		if err := plans.Save(context.Background(), plan); err != nil {
			t.Fatal(err)
		}
	}
	got, err := (&ListCapacityPlans{Plans: plans}).Handle(context.Background(), "SIM1", nil, 2)
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

func (f fakePathLister) ListPage(context.Context, *pagecursor.Cursor, int) ([]processpath.ProcessPath, error) {
	return f.paths, f.err
}

func TestListProcessPaths(t *testing.T) {
	got, err := (&ListProcessPaths{Paths: fakePathLister{}}).Handle(context.Background(), nil, 10)
	if err != nil || got == nil || len(got) != 0 {
		t.Errorf("nil from the port => %#v, %v; want empty non-nil slice", got, err)
	}
	boom := errors.New("boom")
	if _, err := (&ListProcessPaths{Paths: fakePathLister{err: boom}}).Handle(context.Background(), nil, 10); !errors.Is(err, boom) {
		t.Errorf("err = %v, want boom", err)
	}

	repo := memory.NewProcessPathRepo()
	path, _ := processpath.NewProcessPath("pick-rebin-pack", "Pick-Rebin-Pack", []processpath.ProcessType{"PICK", "REBIN", "PACK"})
	if err := repo.Save(context.Background(), path); err != nil {
		t.Fatal(err)
	}
	got, err = (&ListProcessPaths{Paths: repo}).Handle(context.Background(), nil, 10)
	if err != nil || len(got) != 1 || got[0].ID() != "pick-rebin-pack" {
		t.Errorf("got %v, %v; want the registered path", got, err)
	}
}
