package usecases

import (
	"context"
	"errors"
	"testing"

	"github.com/claudioed/warehouse-planning/internal/adapters/outbound/memory"
	"github.com/claudioed/warehouse-planning/internal/domain/processcapacity"
	"github.com/claudioed/warehouse-planning/internal/domain/processflow"
	"github.com/claudioed/warehouse-planning/internal/domain/processpath"
)

func storeFlow(t *testing.T, repo *memory.ProcessFlowRepo, id string, version int64, status processflow.Status, types ...processflow.ProcessType) {
	t.Helper()
	if _, err := repo.Record(context.Background(), flowFixture(t, id, version, status, types...)); err != nil {
		t.Fatal(err)
	}
}

func stepsOf(p *processpath.ProcessPath) string {
	out := ""
	for _, s := range p.Steps() {
		out += string(s) + " "
	}
	return out
}

func TestFlowPathFinder_ActiveResolvesRetiredAndUnknownDoNot(t *testing.T) {
	flows := memory.NewProcessFlowRepo()
	storeFlow(t, flows, "both", 1, processflow.StatusActive, processflow.Pick, processflow.Rebin, processflow.Pack)
	storeFlow(t, flows, "retired", 2, processflow.StatusRetired, processflow.Rebin)
	finder := &FlowPathFinder{Flows: flows}
	ctx := context.Background()

	got, err := finder.FindByID(ctx, "both")
	if err != nil || got == nil || stepsOf(got) != "PICK REBIN PACK " || got.Source() != processpath.SourceFlow || got.ID() != "both" {
		t.Fatalf("both = %+v, %v", got, err)
	}
	if got.Name() != "flow both" {
		t.Fatalf("a flow's path carries the flow's name, got %q", got.Name())
	}
	if got, err := finder.FindByID(ctx, "retired"); got != nil || err != nil {
		t.Fatalf("retired = %+v, %v; want nil, nil", got, err)
	}
	if got, err := finder.FindByID(ctx, "nowhere"); got != nil || err != nil {
		t.Fatalf("unknown = %+v, %v; want nil, nil", got, err)
	}
}

func TestFlowPathFinder_ErrorsAreReturned(t *testing.T) {
	boom := errors.New("down")
	if _, err := (&FlowPathFinder{Flows: failingFlows{err: boom}}).FindByID(context.Background(), "x"); !errors.Is(err, boom) {
		t.Fatalf("flow error = %v, want %v", err, boom)
	}
}

type failingLocal struct{ err error }

func (f failingLocal) FindByID(context.Context, string) (*processpath.ProcessPath, error) {
	return nil, f.err
}

func TestGetProcessPath_ResolvesWithSourceOr404(t *testing.T) {
	flows := memory.NewProcessFlowRepo()
	storeFlow(t, flows, "f", 1, processflow.StatusActive, processflow.Pack)
	uc := &GetProcessPath{Paths: &FlowPathFinder{Flows: flows}}
	ctx := context.Background()
	if p, err := uc.Handle(ctx, "f"); err != nil || p.Source() != processpath.SourceFlow {
		t.Fatalf("f = %+v, %v", p, err)
	}
	if _, err := uc.Handle(ctx, "x"); !errors.Is(err, ErrProcessPathNotFound) {
		t.Fatalf("x = %v, want ErrProcessPathNotFound", err)
	}
	boom := errors.New("down")
	if _, err := (&GetProcessPath{Paths: failingLocal{err: boom}}).Handle(ctx, "x"); !errors.Is(err, boom) {
		t.Fatalf("err = %v, want %v", err, boom)
	}
}

func TestFlowPathLister_ListsActiveFlowsOrderedAndPages(t *testing.T) {
	flows := memory.NewProcessFlowRepo()
	for _, id := range []string{"b", "c", "d", "g", "i"} {
		storeFlow(t, flows, id, 1, processflow.StatusActive, processflow.Pack)
	}
	storeFlow(t, flows, "f", 2, processflow.StatusRetired, processflow.Pack) // never listed
	lister := &FlowPathLister{Flows: flows}
	ctx := context.Background()

	var ids []string
	got, err := lister.ListPage(ctx, nil, 100)
	if err != nil {
		t.Fatal(err)
	}
	for _, p := range got {
		ids = append(ids, p.ID())
	}
	if want := "b c d g i"; joinIDs(ids) != want {
		t.Fatalf("ids = %q, want %q", joinIDs(ids), want)
	}
	// A retired flow is invisible; paging walks the ACTIVE flows only.
	page, err := lister.ListPage(ctx, nil, 2)
	if err != nil || len(page) != 2 || page[0].ID() != "b" || page[1].ID() != "c" {
		t.Fatalf("first page = %+v, %v", page, err)
	}
}

func joinIDs(ids []string) string {
	out := ""
	for i, id := range ids {
		if i > 0 {
			out += " "
		}
		out += id
	}
	return out
}

func TestGetProcessPathCapacity_ComposesAFlowAndReportsTheSource(t *testing.T) {
	ctx := context.Background()
	pcRepo, flows := memory.NewProcessCapacityRepo(), memory.NewProcessFlowRepo()
	start, end := pickZoneAWindowTimes()
	uc := &GetProcessPathCapacity{
		ProcessPaths:      &FlowPathFinder{Flows: flows},
		ProcessCapacities: pcRepo, StationStandards: memory.NewStationStandardRepo(), Tally: memory.NewStorageTallyRepo(),
	}
	cmd := GetProcessPathCapacityCommand{
		ProcessPathID: "pick-rebin-pack", Location: "PATH-ZONE-A", WindowStart: start, WindowEnd: end,
		UnitsPerOrder: f64ptr(2.5), PackagesPerOrder: f64ptr(1),
	}
	if _, err := uc.Handle(ctx, cmd); !errors.Is(err, ErrProcessPathNotFound) {
		t.Fatalf("no flow yet = %v, want ErrProcessPathNotFound", err)
	}

	seedPickRebinPackPathCapacity(t, ctx, pcRepo, flows, start, end) // flow "pick-rebin-pack": PICK REBIN PACK
	got, err := uc.Handle(ctx, cmd)
	if err != nil || got.Source != processpath.SourceFlow || got.BottleneckStep != "REBIN" || got.NormalizedRate.Quantity() != 1000 {
		t.Fatalf("got = %+v, %v; want the worked example composed from the flow", got, err)
	}

	// Revised to PICK -> PACK: no REBIN, so PICK (4000 UNIT/h / 2.5 = 1600 ORDER/h) binds.
	storeFlow(t, flows, "pick-rebin-pack", 2, processflow.StatusActive, processflow.Pick, processflow.Pack)
	flow, err := uc.Handle(ctx, cmd)
	if err != nil || flow.Source != processpath.SourceFlow || flow.BottleneckStep != processcapacity.ProcessType("PICK") ||
		flow.NormalizedRate.Quantity() != 1600 || len(flow.Steps) != 2 {
		t.Fatalf("revised = %+v, %v; want PICK 1600 over two steps", flow, err)
	}

	// Retired: the flow no longer resolves.
	storeFlow(t, flows, "pick-rebin-pack", 3, processflow.StatusRetired, processflow.Pick, processflow.Pack)
	if _, err := uc.Handle(ctx, cmd); !errors.Is(err, ErrProcessPathNotFound) {
		t.Fatalf("after retirement = %v, want ErrProcessPathNotFound", err)
	}
}
