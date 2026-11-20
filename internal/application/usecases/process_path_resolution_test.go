package usecases

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/claudioed/warehouse-planning/internal/adapters/outbound/memory"
	"github.com/claudioed/warehouse-planning/internal/domain/pagecursor"
	"github.com/claudioed/warehouse-planning/internal/domain/processcapacity"
	"github.com/claudioed/warehouse-planning/internal/domain/processflow"
	"github.com/claudioed/warehouse-planning/internal/domain/processpath"
)

func localPath(t *testing.T, repo *memory.ProcessPathRepo, id string, steps ...processpath.ProcessType) {
	t.Helper()
	p, err := processpath.NewProcessPath(id, "Local "+id, steps)
	if err != nil {
		t.Fatal(err)
	}
	if err := repo.Save(context.Background(), p); err != nil {
		t.Fatal(err)
	}
}

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

func TestFlowFirstPathFinder_FlowWinsRetiredFallsBackAndLocalKeepsWorking(t *testing.T) {
	flows, local := memory.NewProcessFlowRepo(), memory.NewProcessPathRepo()
	localPath(t, local, "both", "PICK", "PACK")
	localPath(t, local, "retired", "PICK", "SLAM")
	localPath(t, local, "only-local", "PACK")
	storeFlow(t, flows, "both", 1, processflow.StatusActive, processflow.Pick, processflow.Rebin, processflow.Pack)
	storeFlow(t, flows, "retired", 2, processflow.StatusRetired, processflow.Rebin)
	storeFlow(t, flows, "only-flow", 1, processflow.StatusActive, processflow.Dispatch)
	finder := &FlowFirstPathFinder{Flows: flows, Local: local}
	ctx := context.Background()

	for _, c := range []struct {
		id, steps string
		source    processpath.Source
	}{
		{"both", "PICK REBIN PACK ", processpath.SourceFlow},
		{"retired", "PICK SLAM ", processpath.SourceLocal},
		{"only-local", "PACK ", processpath.SourceLocal},
		{"only-flow", "DISPATCH ", processpath.SourceFlow},
	} {
		got, err := finder.FindByID(ctx, c.id)
		if err != nil || got == nil || stepsOf(got) != c.steps || got.Source() != c.source || got.ID() != c.id {
			t.Fatalf("%s = %+v, %v; want steps %q source %s", c.id, got, err, c.steps, c.source)
		}
	}
	if got, err := finder.FindByID(ctx, "nowhere"); got != nil || err != nil {
		t.Fatalf("unknown = %+v, %v; want nil, nil", got, err)
	}
	if got, _ := finder.FindByID(ctx, "both"); got.Name() != "flow both" {
		t.Fatalf("a flow's path carries the flow's name, got %q", got.Name())
	}
}

func TestFlowFirstPathFinder_NilFlowsIsLocalOnly(t *testing.T) {
	local := memory.NewProcessPathRepo()
	localPath(t, local, "p", "PICK")
	got, err := (&FlowFirstPathFinder{Local: local}).FindByID(context.Background(), "p")
	if err != nil || got == nil || got.Source() != processpath.SourceLocal {
		t.Fatalf("got %+v, %v", got, err)
	}
}

func TestFlowFirstPathFinder_ErrorsAreReturned(t *testing.T) {
	boom := errors.New("down")
	if _, err := (&FlowFirstPathFinder{Flows: failingFlows{err: boom}, Local: memory.NewProcessPathRepo()}).FindByID(context.Background(), "x"); !errors.Is(err, boom) {
		t.Fatalf("flow error = %v, want %v", err, boom)
	}
}

type failingLocal struct{ err error }

func (f failingLocal) FindByID(context.Context, string) (*processpath.ProcessPath, error) {
	return nil, f.err
}

func TestGetProcessPath_ResolvesWithSourceOr404(t *testing.T) {
	flows, local := memory.NewProcessFlowRepo(), memory.NewProcessPathRepo()
	localPath(t, local, "l", "PICK")
	storeFlow(t, flows, "f", 1, processflow.StatusActive, processflow.Pack)
	uc := &GetProcessPath{Paths: &FlowFirstPathFinder{Flows: flows, Local: local}}
	ctx := context.Background()
	if p, err := uc.Handle(ctx, "f"); err != nil || p.Source() != processpath.SourceFlow {
		t.Fatalf("f = %+v, %v", p, err)
	}
	if p, err := uc.Handle(ctx, "l"); err != nil || p.Source() != processpath.SourceLocal {
		t.Fatalf("l = %+v, %v", p, err)
	}
	if _, err := uc.Handle(ctx, "x"); !errors.Is(err, ErrProcessPathNotFound) {
		t.Fatalf("x = %v, want ErrProcessPathNotFound", err)
	}
	boom := errors.New("down")
	if _, err := (&GetProcessPath{Paths: failingLocal{err: boom}}).Handle(ctx, "x"); !errors.Is(err, boom) {
		t.Fatalf("err = %v, want %v", err, boom)
	}
}

func TestFlowFirstPathLister_MergesByIDWithFlowWinningAndPagesExactly(t *testing.T) {
	flows, local := memory.NewProcessFlowRepo(), memory.NewProcessPathRepo()
	for _, id := range []string{"a", "c", "e", "g", "h"} {
		localPath(t, local, id, "PICK")
	}
	for _, id := range []string{"b", "c", "d", "g", "i"} {
		storeFlow(t, flows, id, 1, processflow.StatusActive, processflow.Pack)
	}
	storeFlow(t, flows, "f", 2, processflow.StatusRetired, processflow.Pack)
	lister := &FlowFirstPathLister{Flows: flows, Local: local}
	want := []struct {
		id     string
		source processpath.Source
	}{
		{"a", processpath.SourceLocal}, {"b", processpath.SourceFlow}, {"c", processpath.SourceFlow},
		{"d", processpath.SourceFlow}, {"e", processpath.SourceLocal}, {"g", processpath.SourceFlow},
		{"h", processpath.SourceLocal}, {"i", processpath.SourceFlow},
	}
	for limit := 1; limit <= 10; limit++ {
		got := walkPages(t, lister, limit)
		if len(got) != len(want) {
			t.Fatalf("limit %d: %d paths, want %d (%v)", limit, len(got), len(want), idsOf(got))
		}
		for i, w := range want {
			if got[i].ID() != w.id || got[i].Source() != w.source {
				t.Fatalf("limit %d: item %d = %s/%s, want %s/%s", limit, i, got[i].ID(), got[i].Source(), w.id, w.source)
			}
		}
	}
}

// walkPages follows the keyset cursor until a short page, never exceeding limit.
func walkPages(t *testing.T, lister *FlowFirstPathLister, limit int) []processpath.ProcessPath {
	t.Helper()
	var got []processpath.ProcessPath
	var cursor *pagecursor.Cursor
	for pages := 0; pages < 20; pages++ {
		page, err := lister.ListPage(context.Background(), cursor, limit)
		if err != nil {
			t.Fatal(err)
		}
		if len(page) > limit {
			t.Fatalf("limit %d: a page of %d", limit, len(page))
		}
		got = append(got, page...)
		if len(page) < limit {
			break
		}
		last := page[len(page)-1].ID()
		cursor = &pagecursor.Cursor{Sort: last, ID: last}
	}
	return got
}

func idsOf(ps []processpath.ProcessPath) string {
	out := ""
	for _, p := range ps {
		out += fmt.Sprintf("%s(%s) ", p.ID(), p.Source())
	}
	return out
}

func TestFlowFirstPathLister_NilFlowsAndErrors(t *testing.T) {
	local := memory.NewProcessPathRepo()
	localPath(t, local, "a", "PICK")
	ctx := context.Background()
	if page, err := (&FlowFirstPathLister{Local: local}).ListPage(ctx, nil, 5); err != nil || len(page) != 1 {
		t.Fatalf("local only = %v, %v", page, err)
	}
	boom := errors.New("down")
	if _, err := (&FlowFirstPathLister{Flows: failingFlows{err: boom}, Local: local}).ListPage(ctx, nil, 5); !errors.Is(err, boom) {
		t.Fatalf("flow error = %v", err)
	}
	if page, err := (&FlowFirstPathLister{Flows: memory.NewProcessFlowRepo(), Local: local}).ListPage(ctx, nil, 0); err != nil || len(page) != 0 {
		t.Fatalf("limit 0 = %v, %v", page, err)
	}
}

// docs/adr/0025 decision 2: the capacity composes the steps of the ACTIVE flow
// (the stages' process types in order) and reports its source; a retired flow
// falls back to the local path.
func TestGetProcessPathCapacity_ResolvesFlowFirstAndReportsTheSource(t *testing.T) {
	ctx := context.Background()
	pcRepo, ppRepo, flows := memory.NewProcessCapacityRepo(), memory.NewProcessPathRepo(), memory.NewProcessFlowRepo()
	start, end := pickZoneAWindowTimes()
	seedPickRebinPackPathCapacity(t, ctx, pcRepo, ppRepo, start, end) // local "pick-rebin-pack": PICK REBIN PACK
	uc := &GetProcessPathCapacity{
		ProcessPaths:      &FlowFirstPathFinder{Flows: flows, Local: ppRepo},
		ProcessCapacities: pcRepo, StationStandards: memory.NewStationStandardRepo(), Tally: memory.NewStorageTallyRepo(),
	}
	cmd := GetProcessPathCapacityCommand{
		ProcessPathID: "pick-rebin-pack", Location: "PATH-ZONE-A", WindowStart: start, WindowEnd: end,
		UnitsPerOrder: f64ptr(2.5), PackagesPerOrder: f64ptr(1),
	}

	local, err := uc.Handle(ctx, cmd)
	if err != nil || local.Source != processpath.SourceLocal || local.BottleneckStep != "REBIN" {
		t.Fatalf("local = %+v, %v; want source local bound by REBIN", local, err)
	}

	// An ACTIVE flow of the same id wins: PICK -> PACK has no REBIN, so PACK (1800
	// PACKAGE/h = 1800 ORDER/h) against PICK (4000 UNIT/h / 2.5 = 1600 ORDER/h).
	storeFlow(t, flows, "pick-rebin-pack", 1, processflow.StatusActive, processflow.Pick, processflow.Pack)
	flow, err := uc.Handle(ctx, cmd)
	if err != nil || flow.Source != processpath.SourceFlow || flow.BottleneckStep != processcapacity.ProcessType("PICK") ||
		flow.NormalizedRate.Quantity() != 1600 || len(flow.Steps) != 2 {
		t.Fatalf("flow = %+v, %v; want source flow, PICK 1600 over two steps", flow, err)
	}

	// Retired: the flow no longer resolves, the local path is used again.
	storeFlow(t, flows, "pick-rebin-pack", 2, processflow.StatusRetired, processflow.Pick, processflow.Pack)
	back, err := uc.Handle(ctx, cmd)
	if err != nil || back.Source != processpath.SourceLocal || back.BottleneckStep != "REBIN" || len(back.Steps) != 3 {
		t.Fatalf("after retirement = %+v, %v; want the local path again", back, err)
	}
}

func TestGetProcessPathCapacity_AFlowWithNoLocalPathResolves(t *testing.T) {
	ctx := context.Background()
	pcRepo, ppRepo, flows := memory.NewProcessCapacityRepo(), memory.NewProcessPathRepo(), memory.NewProcessFlowRepo()
	start, end := pickZoneAWindowTimes()
	seedPickRebinPackPathCapacity(t, ctx, pcRepo, ppRepo, start, end)
	storeFlow(t, flows, "flow-only", 1, processflow.StatusActive, processflow.Pick, processflow.Rebin, processflow.Pack)
	uc := &GetProcessPathCapacity{
		ProcessPaths:      &FlowFirstPathFinder{Flows: flows, Local: ppRepo},
		ProcessCapacities: pcRepo, StationStandards: memory.NewStationStandardRepo(), Tally: memory.NewStorageTallyRepo(),
	}
	got, err := uc.Handle(ctx, GetProcessPathCapacityCommand{
		ProcessPathID: "flow-only", Location: "PATH-ZONE-A", WindowStart: start, WindowEnd: end,
		UnitsPerOrder: f64ptr(2.5), PackagesPerOrder: f64ptr(1),
	})
	if err != nil || got.Source != processpath.SourceFlow || got.BottleneckStep != "REBIN" || got.NormalizedRate.Quantity() != 1000 {
		t.Fatalf("got %+v, %v; want the flow composed like the worked example", got, err)
	}
}
