package usecases

import (
	"context"
	"testing"
	"time"

	"github.com/claudioed/warehouse-planning/internal/adapters/outbound/memory"
	"github.com/claudioed/warehouse-planning/internal/domain/labourplan"
	"github.com/claudioed/warehouse-planning/internal/domain/workloadforecast"
)

// The requirement side keys every process path on its canonical code
// (trimmed, upper case): any spelling of a path reaches the same forecast,
// plan and commitment.

func TestCanonicalPath_AForecastPutAsPickDerivesTheHeadsOfAPlanPutAsPICK(t *testing.T) {
	f := newLabourFixture()
	receive := &ReceiveWorkloadForecast{
		Forecasts: f.forecasts, Outbox: f.outbox, Encoder: fakeEncoder{},
		UnitOfWork: memory.NewUnitOfWork(f.forecasts, f.outbox), Now: func() time.Time { return planCommitAt },
	}
	if _, err := receive.Handle(context.Background(), ReceiveWorkloadForecastCommand{
		SiteID: "SIM1", PathID: "Pick",
		Buckets: []workloadforecast.Bucket{{CPT: shiftStart, Quantity: 300}, {CPT: shiftStart.Add(time.Hour), Quantity: 500}},
	}); err != nil {
		t.Fatal(err)
	}
	for _, path := range []string{"PICK", " pick "} {
		cmd := planCmd(nil)
		cmd.PathID = path
		res, err := f.commit.Handle(context.Background(), cmd)
		if err != nil {
			t.Fatalf("commit %q: %v", path, err)
		}
		// ceil(800 units / (10 units/h x 8 h)) = 10.
		if spec := res.Plan.Spec(); spec.PathID != "PICK" || spec.PlannedHeads != 10 || spec.HeadsSource != labourplan.HeadsFromForecast {
			t.Fatalf("commit %q: spec = %+v, want PICK with 10 heads from the forecast", path, spec)
		}
	}
	got, err := (&GetWorkloadForecast{Forecasts: f.forecasts}).Handle(context.Background(), "SIM1", "pick")
	if err != nil || got.PathID() != "PICK" {
		t.Fatalf("GET forecast as pick = %v, %v, want the PICK forecast", got, err)
	}
}

func TestCanonicalPath_EverySpellingReadsAndRevisesTheSamePlan(t *testing.T) {
	f := newLabourFixture()
	cmd := planCmd(heads(7))
	cmd.PathID = "PICK"
	if _, err := f.commit.Handle(context.Background(), cmd); err != nil {
		t.Fatal(err)
	}
	cmd.PathID = "pick"
	res, err := f.commit.Handle(context.Background(), cmd)
	if err != nil || res.Created || res.Changed || res.Plan.Version() != 1 {
		t.Fatalf("an equal commit as pick = %+v, %v, want the stored plan unchanged", res, err)
	}
	get := &GetLabourPlan{Plans: f.plans}
	for _, path := range []string{"pick", "PICK", " Pick "} {
		view, err := get.Handle(context.Background(), "SIM1", "S1", path)
		if err != nil || view.Plan.Spec().PathID != "PICK" {
			t.Fatalf("GET plan as %q = %+v, %v, want the PICK plan", path, view, err)
		}
	}
}

func TestCanonicalPath_ACommitmentForPICKMeetsThePlanPutAsPick(t *testing.T) {
	plans, commitments, ob := memory.NewLabourPlanRepo(), memory.NewCommittedHeadsRepo(), memory.NewOutboxRepo()
	uow := memory.NewUnitOfWork(plans, commitments, ob)
	commit := &CommitLabourPlan{
		Plans: plans, Outbox: ob, Encoder: fakeEncoder{}, UnitOfWork: uow, Commitments: commitments,
		Now: func() time.Time { return planCommitAt },
	}
	cmd := planCmd(heads(7))
	cmd.PathID = "pick"
	if _, err := commit.Handle(context.Background(), cmd); err != nil {
		t.Fatal(err)
	}
	record := &RecordShiftCommitment{
		Commitments: commitments, Plans: plans, Outbox: ob, Encoder: fakeEncoder{}, UnitOfWork: uow,
		Now: func() time.Time { return planCommitAt },
	}
	if err := record.Handle(context.Background(), RecordShiftCommitmentCommand{
		SiteID: "SIM1", ShiftID: "S1", PathID: "PICK", CommittedHeads: 10, ObservedAt: planCommitAt,
	}); err != nil {
		t.Fatal(err)
	}
	drifts := 0
	for _, m := range ob.Messages() {
		if m.EventType == labourplan.EventLabourPlanDriftDetected {
			drifts++
			if m.Subject != "SIM1/S1/PICK" {
				t.Fatalf("drift subject = %q, want SIM1/S1/PICK", m.Subject)
			}
		}
	}
	if drifts != 1 {
		t.Fatalf("%d drifts queued, want 1 (%+v)", drifts, ob.Messages())
	}
	c, found, err := commitments.Get(context.Background(), "SIM1", "S1", "PICK")
	if err != nil || !found || c.PathID != "PICK" || c.Heads != 10 || !c.HasLastDrift || c.LastDriftHeads != 3 {
		t.Fatalf("commitment = %+v (found %v, %v), want PICK, 10 heads, drift 3 recorded", c, found, err)
	}
}
