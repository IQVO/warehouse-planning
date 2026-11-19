package usecases

import (
	"context"
	"errors"
	"testing"

	"github.com/claudioed/warehouse-planning/internal/domain/labourplan"
)

// The plan trigger of the labour plan drift (docs/adr/0023 decision 3): a
// CHANGED plan is compared with the heads workforce-management committed for
// the key, in the unit of work of the plan's save.

func TestCommitLabourPlan_ANewPlanAgainstACommitmentRaisesTheDrift(t *testing.T) {
	f := newDriftFixture()
	f.commitHeads(t, 10)
	f.plan(t, 7)
	f.wantDrifts(t, 1)
	want := labourplan.DriftDetected{SiteID: "SIM1", ShiftID: "S1", PathID: "PICK", PlannedHeads: 7, CommittedHeads: 10, DriftHeads: 3, At: planCommitAt}
	if f.drifts[0] != want {
		t.Fatalf("drift = %+v, want %+v", f.drifts[0], want)
	}
	f.wantRecorded(t, 3)
	if c, _ := f.stored(t); c.Heads != 10 || !c.ObservedAt.Equal(driftObservedAt) {
		t.Fatalf("the plan changed the commitment: %+v", c)
	}
}

func TestCommitLabourPlan_ARevisedPlanWithANewDriftRaisesIt(t *testing.T) {
	f := newDriftFixture()
	f.commitHeads(t, 10)
	f.plan(t, 7)
	res := f.plan(t, 9)
	if !res.Changed || res.Plan.Version() != 2 {
		t.Fatalf("revise = %+v", res)
	}
	f.wantDrifts(t, 2)
	if f.drifts[1].DriftHeads != 1 || f.drifts[1].PlannedHeads != 9 {
		t.Fatalf("second drift = %+v, want +1 (9 planned)", f.drifts[1])
	}
	f.wantRecorded(t, 1)
}

func TestCommitLabourPlan_AnUnchangedPutRaisesNothing(t *testing.T) {
	f := newDriftFixture()
	f.commitHeads(t, 10)
	f.plan(t, 7)
	before := len(f.outbox.Messages())
	// A drift recorded elsewhere proves the unchanged PUT never even compared.
	if err := f.commitments.RecordDrift(context.Background(), "SIM1", "S1", "PICK", 42); err != nil {
		t.Fatal(err)
	}
	if res := f.plan(t, 7); res.Changed {
		t.Fatalf("the same PUT changed the plan: %+v", res)
	}
	if after := len(f.outbox.Messages()); after != before {
		t.Fatalf("outbox %d -> %d: an unchanged PUT queued something", before, after)
	}
	f.wantRecorded(t, 42)
}

func TestCommitLabourPlan_AChangeKeepingTheHeadsDoesNotRaiseTheSameDriftAgain(t *testing.T) {
	f := newDriftFixture()
	f.commitHeads(t, 10)
	f.plan(t, 7)
	cmd := planCmd(heads(7))
	cmd.RateUnitsPerHour = 12
	res, err := f.commit.Handle(context.Background(), cmd)
	if err != nil || !res.Changed {
		t.Fatalf("revise = %+v, %v", res, err)
	}
	f.wantDrifts(t, 1)
	f.wantRecorded(t, 3)
}

func TestCommitLabourPlan_AClearedDriftThatReturnsIsRaisedAgain(t *testing.T) {
	f := newDriftFixture()
	f.commitHeads(t, 10)
	f.plan(t, 7)
	f.plan(t, 10)
	f.wantDrifts(t, 1)
	f.wantRecorded(t, 0)
	f.plan(t, 7)
	f.wantDrifts(t, 2)
	f.wantRecorded(t, 3)
}

func TestCommitLabourPlan_NoCommitmentSurfacesNothing(t *testing.T) {
	f := newDriftFixture()
	f.plan(t, 7)
	f.plan(t, 9)
	f.wantDrifts(t, 0)
	if _, found := f.stored(t); found {
		t.Fatal("a plan commit created a commitment")
	}
}

func TestCommitLabourPlan_NoCommitmentsWiredSurfacesNothing(t *testing.T) {
	f := newDriftFixture()
	f.commitHeads(t, 10)
	f.commit.Commitments = nil
	f.plan(t, 7)
	f.wantDrifts(t, 0)
	if c, _ := f.stored(t); c.HasLastDrift {
		t.Fatalf("a drift was recorded without the trigger wired: %+v", c)
	}
}

func TestCommitLabourPlan_ADriftFailureRollsThePlanBack(t *testing.T) {
	for name, fail := range map[string]failingCommitments{
		"get":    {getErr: errLabourBoom},
		"record": {recordErr: errLabourBoom},
	} {
		t.Run(name, func(t *testing.T) {
			f := newDriftFixture()
			f.commitHeads(t, 10)
			fail.CommittedHeadsRepo = f.commitments
			f.commit.Commitments = fail
			if _, err := f.commit.Handle(context.Background(), planCmd(heads(7))); !errors.Is(err, errLabourBoom) {
				t.Fatalf("err = %v, want the injected failure", err)
			}
			if _, found, _ := f.plans.Get(context.Background(), "SIM1", "S1", "PICK"); found {
				t.Fatal("the plan survived the rolled-back unit of work")
			}
			if len(f.outbox.Messages()) != 0 {
				t.Fatalf("outbox = %+v after the rollback", f.outbox.Messages())
			}
		})
	}
}
