package usecases

import (
	"context"
	"errors"
	"testing"

	"github.com/claudioed/warehouse-planning/internal/adapters/outbound/memory"
	"github.com/claudioed/warehouse-planning/internal/domain/pagecursor"
	"github.com/claudioed/warehouse-planning/internal/domain/processflow"
)

const flowConsumer = "process-flow-consumer"

func flowFixture(t *testing.T, id string, version int64, status processflow.Status, types ...processflow.ProcessType) processflow.Flow {
	t.Helper()
	stages := make([]processflow.Stage, 0, len(types))
	for _, pt := range types {
		stages = append(stages, processflow.Stage{PathID: string(pt), ProcessType: pt})
	}
	f, err := processflow.New(processflow.Params{
		FlowID: id, Name: "flow " + id, Stages: stages, Status: status, Version: version,
		UpdatedAt: planWindowStart,
	})
	if err != nil {
		t.Fatal(err)
	}
	return f
}

type failingFlows struct{ err error }

func (f failingFlows) Record(context.Context, processflow.Flow) (bool, error) { return false, f.err }

func (f failingFlows) FindActive(context.Context, string) (*processflow.Flow, error) {
	return nil, f.err
}

func (f failingFlows) ListActivePage(context.Context, *pagecursor.Cursor, int) ([]processflow.Flow, error) {
	return nil, f.err
}

func TestProcessFlowRepoFindAndListActive(t *testing.T) {
	repo, ctx := memory.NewProcessFlowRepo(), context.Background()
	for _, f := range []processflow.Flow{
		flowFixture(t, "b", 1, processflow.StatusActive, processflow.Pick),
		flowFixture(t, "a", 1, processflow.StatusActive, processflow.Pack),
		flowFixture(t, "c", 2, processflow.StatusRetired, processflow.Pick),
		flowFixture(t, "d", 1, processflow.StatusActive, processflow.Pick),
	} {
		if _, err := repo.Record(ctx, f); err != nil {
			t.Fatal(err)
		}
	}
	if f, _ := repo.FindActive(ctx, "c"); f != nil {
		t.Fatal("a RETIRED flow must not resolve")
	}
	if f, _ := repo.FindActive(ctx, "nope"); f != nil {
		t.Fatal("an unknown flow must not resolve")
	}
	if f, _ := repo.FindActive(ctx, "a"); f == nil || f.ID() != "a" {
		t.Fatal("an ACTIVE flow must resolve")
	}
	page, _ := repo.ListActivePage(ctx, nil, 2)
	if len(page) != 2 || page[0].ID() != "a" || page[1].ID() != "b" {
		t.Fatalf("first page = %v, want a, b (retired c excluded)", page)
	}
	rest, _ := repo.ListActivePage(ctx, &pagecursor.Cursor{ID: "b"}, 5)
	if len(rest) != 1 || rest[0].ID() != "d" {
		t.Fatalf("after b = %v, want d only", rest)
	}
}

func TestProcessFlowRepoSnapshotRestores(t *testing.T) {
	repo, ctx := memory.NewProcessFlowRepo(), context.Background()
	_, _ = repo.Record(ctx, flowFixture(t, "a", 1, processflow.StatusActive, processflow.Pick))
	restore := repo.Snapshot()
	_, _ = repo.Record(ctx, flowFixture(t, "a", 2, processflow.StatusActive, processflow.Pack))
	_, _ = repo.Record(ctx, flowFixture(t, "b", 1, processflow.StatusActive, processflow.Pick))
	restore()
	if a, _ := repo.Get("a"); repo.Len() != 1 || a.Version() != 1 {
		t.Fatal("Snapshot's restore did not undo the later records")
	}
}

func TestRecordProcessFlow_VersionGuard(t *testing.T) {
	repo, processed := memory.NewProcessFlowRepo(), memory.NewProcessedEventRepo()
	rec := &RecordProcessFlow{UoW: memory.NewUnitOfWork(repo, processed), ProcessedEvents: processed, Flows: repo}
	ctx := context.Background()
	pick, pack := processflow.Pick, processflow.Pack
	for _, step := range []struct {
		name, eventID string
		f             processflow.Flow
		want          RecordOutcome
		wantVersion   int64
	}{
		{"first message of a Retired type stores a RETIRED row", "e1", flowFixture(t, "f-retired", 3, processflow.StatusRetired, pick, pack), OutcomeApplied, 3},
		{"first message for an unknown flow", "e2", flowFixture(t, "f1", 1, processflow.StatusActive, pick, pack), OutcomeApplied, 1},
		{"greater version replaces", "e3", flowFixture(t, "f1", 2, processflow.StatusActive, pick, processflow.Rebin, pack), OutcomeApplied, 2},
		{"equal version is a no-op", "e4", flowFixture(t, "f1", 2, processflow.StatusActive, pick), OutcomeStale, 2},
		{"lower version is a no-op", "e5", flowFixture(t, "f1", 1, processflow.StatusActive, pick), OutcomeStale, 2},
		{"same CloudEvents id is a duplicate", "e3", flowFixture(t, "f1", 9, processflow.StatusActive, pick), OutcomeDuplicate, 2},
		{"retirement applies", "e6", flowFixture(t, "f1", 3, processflow.StatusRetired, pick, processflow.Rebin, pack), OutcomeApplied, 3},
		{"a revision after retirement with a lower version is stale", "e7", flowFixture(t, "f1", 2, processflow.StatusActive, pick), OutcomeStale, 3},
	} {
		got, err := rec.Handle(ctx, flowConsumer, step.eventID, step.f)
		if err != nil || got != step.want {
			t.Fatalf("%s: %s, %v; want %s", step.name, got, err, step.want)
		}
		if stored, _ := repo.Get(step.f.ID()); stored.Version() != step.wantVersion {
			t.Fatalf("%s: stored version %d, want %d", step.name, stored.Version(), step.wantVersion)
		}
	}
	if !processed.Has(flowConsumer, "e5") {
		t.Fatal("a stale message must still be claimed")
	}
	if f, _ := repo.Get("f1"); f.Active() || len(f.Steps()) != 3 {
		t.Fatalf("f1 = %+v, want the RETIRED v3 state", f)
	}
}

func TestRecordProcessFlow_FailuresRollTheClaimBack(t *testing.T) {
	boom := errors.New("database went away")
	ctx := context.Background()
	f := flowFixture(t, "f1", 1, processflow.StatusActive, processflow.Pick)

	repo := memory.NewProcessFlowRepo()
	claimFails := &RecordProcessFlow{UoW: memory.NewUnitOfWork(repo), ProcessedEvents: failingClaim{err: boom}, Flows: repo}
	if _, err := claimFails.Handle(ctx, "c", "e1", f); !errors.Is(err, boom) {
		t.Fatalf("claim failure = %v, want it returned", err)
	}
	if repo.Len() != 0 {
		t.Fatal("a flow was recorded without a claim")
	}

	processed := memory.NewProcessedEventRepo()
	recordFails := &RecordProcessFlow{UoW: memory.NewUnitOfWork(processed), ProcessedEvents: processed, Flows: failingFlows{err: boom}}
	if _, err := recordFails.Handle(ctx, "c", "e1", f); !errors.Is(err, boom) {
		t.Fatalf("record failure = %v, want it returned", err)
	}
	if processed.Has("c", "e1") {
		t.Fatal("a failed record left its claim behind: the redelivery would be skipped")
	}
}
