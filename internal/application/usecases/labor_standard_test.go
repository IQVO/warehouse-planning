package usecases

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/claudioed/warehouse-planning/internal/adapters/outbound/memory"
	"github.com/claudioed/warehouse-planning/internal/domain/laborstandard"
)

func laborStandardFixture(t *testing.T, id string, seconds int64) laborstandard.Standard {
	t.Helper()
	s, err := laborstandard.New(laborstandard.Params{
		StandardID: id, TaskType: "PICK", ExpectedSeconds: seconds,
		EffectiveFrom: planWindowStart.Add(-24 * time.Hour), AsOf: planWindowStart.Add(-24 * time.Hour),
	})
	if err != nil {
		t.Fatal(err)
	}
	return s
}

type failingLaborStandards struct{ err error }

func (f failingLaborStandards) Record(context.Context, laborstandard.Standard) (bool, error) {
	return false, f.err
}

func (f failingLaborStandards) InForce(context.Context, string, string, time.Time) (laborstandard.Standard, bool, error) {
	return laborstandard.Standard{}, false, f.err
}

func TestRecordLaborStandard_AppliedDuplicateAndStale(t *testing.T) {
	repo, processed := memory.NewLaborStandardRepo(), memory.NewProcessedEventRepo()
	rec := &RecordLaborStandard{UoW: memory.NewUnitOfWork(repo, processed), ProcessedEvents: processed, Standards: repo}
	ctx := context.Background()
	for _, step := range []struct {
		eventID string
		s       laborstandard.Standard
		want    RecordOutcome
	}{
		{"evt-1", laborStandardFixture(t, "std-1", 45), OutcomeApplied},
		{"evt-1", laborStandardFixture(t, "std-9", 45), OutcomeDuplicate},
		{"evt-2", laborStandardFixture(t, "std-1", 60), OutcomeStale},
	} {
		got, err := rec.Handle(ctx, "labor-standard-consumer", step.eventID, step.s)
		if err != nil || got != step.want {
			t.Fatalf("%s/%s = %s, %v; want %s", step.eventID, step.s.ID(), got, err, step.want)
		}
	}
	if s, _ := repo.Get("std-1"); repo.Len() != 1 || s.ExpectedSeconds() != 45 {
		t.Fatalf("copy = %d records, std-1 = %+v; want only the first record", repo.Len(), s)
	}
	if !processed.Has("labor-standard-consumer", "evt-2") {
		t.Fatal("a stale record must still be claimed (it is never redelivered forever)")
	}
}

func TestRecordLaborStandard_FailuresRollTheClaimBack(t *testing.T) {
	boom := errors.New("database went away")
	ctx := context.Background()

	repo := memory.NewLaborStandardRepo()
	claimFails := &RecordLaborStandard{UoW: memory.NewUnitOfWork(repo), ProcessedEvents: failingClaim{err: boom}, Standards: repo}
	if _, err := claimFails.Handle(ctx, "c", "evt-1", laborStandardFixture(t, "std-1", 45)); !errors.Is(err, boom) {
		t.Fatalf("claim failure = %v, want it returned", err)
	}
	if repo.Len() != 0 {
		t.Fatal("a standard was recorded without a claim")
	}

	processed := memory.NewProcessedEventRepo()
	recordFails := &RecordLaborStandard{UoW: memory.NewUnitOfWork(processed), ProcessedEvents: processed, Standards: failingLaborStandards{err: boom}}
	if _, err := recordFails.Handle(ctx, "c", "evt-1", laborStandardFixture(t, "std-1", 45)); !errors.Is(err, boom) {
		t.Fatalf("record failure = %v, want it returned", err)
	}
	if processed.Has("c", "evt-1") {
		t.Fatal("a failed record left its claim behind: the redelivery would be skipped")
	}
}

func TestLaborStandardRepoSnapshotRestores(t *testing.T) {
	repo := memory.NewLaborStandardRepo()
	ctx := context.Background()
	if _, err := repo.Record(ctx, laborStandardFixture(t, "std-1", 45)); err != nil {
		t.Fatal(err)
	}
	restore := repo.Snapshot()
	if _, err := repo.Record(ctx, laborStandardFixture(t, "std-2", 40)); err != nil {
		t.Fatal(err)
	}
	restore()
	if _, ok := repo.Get("std-2"); ok || repo.Len() != 1 {
		t.Fatal("Snapshot's restore did not undo the later record")
	}
	got, ok, err := repo.InForce(ctx, "SIM1", "PICK", planWindowStart)
	if err != nil || !ok || got.ID() != "std-1" {
		t.Fatalf("InForce = %+v %v %v, want std-1", got, ok, err)
	}
}
