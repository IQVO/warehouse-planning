package repocontract

import (
	"testing"
	"time"

	"github.com/claudioed/warehouse-planning/internal/application/ports"
	"github.com/claudioed/warehouse-planning/internal/domain/pagecursor"
	"github.com/claudioed/warehouse-planning/internal/domain/processflow"
)

// RunProcessFlows runs the ports.ProcessFlowRepository contract (docs/adr/0025)
// against one adapter: the memory test double and the Postgres adapter must
// answer it identically. newRepo returns an EMPTY repository per call.
func RunProcessFlows(t *testing.T, newRepo func(*testing.T) ports.ProcessFlowRepository) {
	t.Helper()
	for _, c := range processFlowCases {
		t.Run(c.name, func(t *testing.T) { c.run(t, newRepo(t)) })
	}
}

func flowOf(t *testing.T, id string, version int64, status processflow.Status, types ...processflow.ProcessType) processflow.Flow {
	t.Helper()
	stages := make([]processflow.Stage, 0, len(types))
	for i, pt := range types {
		stages = append(stages, processflow.Stage{PathID: string(rune('a'+i)) + "-" + string(pt), ProcessType: pt})
	}
	f, err := processflow.New(processflow.Params{
		FlowID: id, Name: "Flow " + id, Stages: stages, Status: status, Version: version,
		UpdatedAt: T0.Add(time.Duration(version) * time.Minute),
	})
	noErr(t, err)
	return f
}

func recorded(t *testing.T, repo ports.ProcessFlowRepository, f processflow.Flow) bool {
	t.Helper()
	applied, err := repo.Record(ctx, f)
	noErr(t, err)
	return applied
}

func activeOf(t *testing.T, repo ports.ProcessFlowRepository, id string) *processflow.Flow {
	t.Helper()
	f, err := repo.FindActive(ctx, id)
	noErr(t, err)
	return f
}

type flowCase struct {
	name string
	run  func(*testing.T, ports.ProcessFlowRepository)
}

var processFlowCases = []flowCase{
	{"a flow is stored with its ordered stages, name, status and version", func(t *testing.T, repo ports.ProcessFlowRepository) {
		if !recorded(t, repo, flowOf(t, "f1", 1, processflow.StatusActive, processflow.Pick, processflow.Rebin, processflow.Pack)) {
			t.Fatal("the first message of an unknown flow must be applied")
		}
		got := activeOf(t, repo, "f1")
		if got == nil || got.Name() != "Flow f1" || got.Version() != 1 || got.Status() != processflow.StatusActive {
			t.Fatalf("stored flow = %+v", got)
		}
		stages := got.Stages()
		if len(stages) != 3 || stages[0].ProcessType != processflow.Pick || stages[1].ProcessType != processflow.Rebin ||
			stages[2].ProcessType != processflow.Pack || stages[1].PathID != "b-REBIN" {
			t.Fatalf("stages = %+v, want PICK REBIN PACK in order with their path ids", stages)
		}
		if !got.UpdatedAt().Equal(T0.Add(time.Minute)) {
			t.Fatalf("updated_at = %v", got.UpdatedAt())
		}
	}},
	{"only a greater version replaces the stored flow", func(t *testing.T, repo ports.ProcessFlowRepository) {
		recorded(t, repo, flowOf(t, "f1", 2, processflow.StatusActive, processflow.Pick, processflow.Pack))
		for _, v := range []int64{1, 2} {
			if recorded(t, repo, flowOf(t, "f1", v, processflow.StatusActive, processflow.Slam)) {
				t.Fatalf("version %d over a stored version 2 must be a no-op", v)
			}
		}
		if got := activeOf(t, repo, "f1"); got.Version() != 2 || len(got.Steps()) != 2 {
			t.Fatalf("stored flow = %+v, want the untouched version 2", got)
		}
		if !recorded(t, repo, flowOf(t, "f1", 3, processflow.StatusActive, processflow.Slam)) {
			t.Fatal("version 3 over a stored version 2 must be applied")
		}
		if got := activeOf(t, repo, "f1"); got.Version() != 3 || len(got.Steps()) != 1 || got.Steps()[0] != processflow.Slam {
			t.Fatalf("stored flow = %+v, want version 3 with SLAM", got)
		}
	}},
	{"a retired flow does not resolve and a first retired message is stored", func(t *testing.T, repo ports.ProcessFlowRepository) {
		recorded(t, repo, flowOf(t, "f1", 1, processflow.StatusActive, processflow.Pick))
		if !recorded(t, repo, flowOf(t, "f1", 2, processflow.StatusRetired, processflow.Pick)) {
			t.Fatal("the retirement must be applied")
		}
		if activeOf(t, repo, "f1") != nil {
			t.Fatal("a RETIRED flow must not resolve")
		}
		if !recorded(t, repo, flowOf(t, "f2", 5, processflow.StatusRetired, processflow.Pick)) {
			t.Fatal("a first RETIRED message must be stored")
		}
		if recorded(t, repo, flowOf(t, "f2", 4, processflow.StatusActive, processflow.Pick)) || activeOf(t, repo, "f2") != nil {
			t.Fatal("an older ACTIVE message must not bring a retired flow back")
		}
	}},
	{"an unknown flow does not resolve", func(t *testing.T, repo ports.ProcessFlowRepository) {
		if activeOf(t, repo, "nope") != nil {
			t.Fatal("an unknown flow resolved")
		}
	}},
	{"the active flows list in id order, keyset-paged, retired ones excluded", func(t *testing.T, repo ports.ProcessFlowRepository) {
		for _, f := range []processflow.Flow{
			flowOf(t, "c", 1, processflow.StatusActive, processflow.Pack),
			flowOf(t, "a", 1, processflow.StatusActive, processflow.Pick),
			flowOf(t, "d", 3, processflow.StatusRetired, processflow.Pick),
			flowOf(t, "b", 1, processflow.StatusActive, processflow.Rebin),
		} {
			recorded(t, repo, f)
		}
		ids := func(after *pagecursor.Cursor, limit int) []string {
			page, err := repo.ListActivePage(ctx, after, limit)
			noErr(t, err)
			out := []string{}
			for _, f := range page {
				out = append(out, f.ID())
			}
			return out
		}
		if got := ids(nil, 10); len(got) != 3 || got[0] != "a" || got[1] != "b" || got[2] != "c" {
			t.Fatalf("all = %v, want a b c", got)
		}
		if got := ids(nil, 2); len(got) != 2 || got[1] != "b" {
			t.Fatalf("first page = %v, want a b", got)
		}
		if got := ids(&pagecursor.Cursor{ID: "b"}, 2); len(got) != 1 || got[0] != "c" {
			t.Fatalf("after b = %v, want c", got)
		}
		if got := ids(&pagecursor.Cursor{ID: "c"}, 2); got == nil || len(got) != 0 {
			t.Fatalf("after c = %v, want an empty, non-nil page", got)
		}
	}},
}
