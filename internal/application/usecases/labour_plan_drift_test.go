package usecases

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/claudioed/warehouse-planning/internal/adapters/outbound/memory"
	"github.com/claudioed/warehouse-planning/internal/application/outbox"
	"github.com/claudioed/warehouse-planning/internal/domain/labourplan"
)

var (
	driftObservedAt = time.Date(2026, 11, 19, 5, 30, 0, 0, time.UTC)
	driftDetectedAt = time.Date(2026, 11, 19, 5, 45, 0, 0, time.UTC)
)

// driftCapture is fakeEncoder that also keeps every LabourPlanDriftDetected
// it encoded, so a test can read the event's heads.
type driftCapture struct {
	fakeEncoder
	drifts *[]labourplan.DriftDetected
}

func (c driftCapture) EncodeLabourPlanDriftDetected(events ...labourplan.DriftDetected) ([]outbox.Message, error) {
	if c.err == nil {
		*c.drifts = append(*c.drifts, events...)
	}
	return c.fakeEncoder.EncodeLabourPlanDriftDetected(events...)
}

// failingCommitments fails the operation whose error is set.
type failingCommitments struct {
	*memory.CommittedHeadsRepo
	getErr, saveErr, recordErr error
}

func (f failingCommitments) Get(ctx context.Context, site, shift, path string) (labourplan.Commitment, bool, error) {
	if f.getErr != nil {
		return labourplan.Commitment{}, false, f.getErr
	}
	return f.CommittedHeadsRepo.Get(ctx, site, shift, path)
}

func (f failingCommitments) SaveCommitment(ctx context.Context, c labourplan.Commitment) error {
	if f.saveErr != nil {
		return f.saveErr
	}
	return f.CommittedHeadsRepo.SaveCommitment(ctx, c)
}

func (f failingCommitments) RecordDrift(ctx context.Context, site, shift, path string, drift int) error {
	if f.recordErr != nil {
		return f.recordErr
	}
	return f.CommittedHeadsRepo.RecordDrift(ctx, site, shift, path, drift)
}

// driftFixture wires both drift triggers -- the commitment (RecordShiftCommitment)
// and the plan (CommitLabourPlan) -- over the same in-memory stores and unit of work.
type driftFixture struct {
	*labourFixture
	commitments *memory.CommittedHeadsRepo
	drifts      []labourplan.DriftDetected
	record      *RecordShiftCommitment
}

func newDriftFixture() *driftFixture {
	f := &driftFixture{labourFixture: newLabourFixture(), commitments: memory.NewCommittedHeadsRepo()}
	enc := driftCapture{drifts: &f.drifts}
	uow := memory.NewUnitOfWork(f.plans, f.forecasts, f.outbox, f.commitments)
	f.commit.Encoder, f.commit.UnitOfWork, f.commit.Commitments = enc, uow, f.commitments
	f.record = &RecordShiftCommitment{
		Commitments: f.commitments, Plans: f.plans, Outbox: f.outbox, Encoder: enc, UnitOfWork: uow,
		Now: func() time.Time { return driftDetectedAt },
	}
	return f
}

// plan commits the SIM1/S1/PICK labour plan with n planned heads.
func (f *driftFixture) plan(t *testing.T, n int) CommitLabourPlanResult {
	t.Helper()
	res, err := f.commit.Handle(context.Background(), planCmd(heads(n)))
	if err != nil {
		t.Fatal(err)
	}
	return res
}

// commitHeads records workforce-management committing n heads for SIM1/S1/PICK.
func (f *driftFixture) commitHeads(t *testing.T, n int) {
	t.Helper()
	if err := f.record.Handle(context.Background(), commitmentCmd(n)); err != nil {
		t.Fatal(err)
	}
}

func commitmentCmd(n int) RecordShiftCommitmentCommand {
	return RecordShiftCommitmentCommand{SiteID: "SIM1", ShiftID: "S1", PathID: "PICK", CommittedHeads: n, ObservedAt: driftObservedAt}
}

// driftMessages counts the LabourPlanDriftDetected rows in the outbox.
func (f *driftFixture) driftMessages() int {
	n := 0
	for _, m := range f.outbox.Messages() {
		if m.EventType == labourplan.EventLabourPlanDriftDetected {
			n++
		}
	}
	return n
}

// stored is the SIM1/S1/PICK commitment.
func (f *driftFixture) stored(t *testing.T) (labourplan.Commitment, bool) {
	t.Helper()
	c, found, err := f.commitments.Get(context.Background(), "SIM1", "S1", "PICK")
	if err != nil {
		t.Fatal(err)
	}
	return c, found
}

func (f *driftFixture) wantRecorded(t *testing.T, drift int) {
	t.Helper()
	c, found := f.stored(t)
	if !found || !c.HasLastDrift || c.LastDriftHeads != drift {
		t.Fatalf("stored commitment = %+v (found %v); want last drift %d recorded", c, found, drift)
	}
}

func (f *driftFixture) wantDrifts(t *testing.T, n int) {
	t.Helper()
	if got := f.driftMessages(); got != n || len(f.drifts) != n {
		t.Fatalf("%d LabourPlanDriftDetected rows in the outbox (%d encoded), want %d", got, len(f.drifts), n)
	}
}

func TestRecordShiftCommitment_NoPlanYetRecordsOnlyTheCommitment(t *testing.T) {
	f := newDriftFixture()
	f.commitHeads(t, 10)
	c, found := f.stored(t)
	if !found || c.Heads != 10 || !c.ObservedAt.Equal(driftObservedAt) || c.HasLastDrift {
		t.Fatalf("stored = %+v (found %v); want 10 heads observed at the event time and no drift", c, found)
	}
	f.wantDrifts(t, 0)
	if len(f.outbox.Messages()) != 0 {
		t.Fatalf("outbox = %+v; a commitment without a plan queues nothing", f.outbox.Messages())
	}
}

func TestRecordShiftCommitment_FirstDriftIsRaisedAndRecorded(t *testing.T) {
	f := newDriftFixture()
	f.plan(t, 7)
	f.commitHeads(t, 10)
	f.wantDrifts(t, 1)
	want := labourplan.DriftDetected{SiteID: "SIM1", ShiftID: "S1", PathID: "PICK", PlannedHeads: 7, CommittedHeads: 10, DriftHeads: 3, At: driftDetectedAt}
	if f.drifts[0] != want {
		t.Fatalf("drift = %+v, want %+v", f.drifts[0], want)
	}
	f.wantRecorded(t, 3)
	if p, _, _ := f.plans.Get(context.Background(), "SIM1", "S1", "PICK"); p.Spec().PlannedHeads != 7 || p.Version() != 1 {
		t.Fatalf("the commitment changed the plan: %+v v%d", p.Spec(), p.Version())
	}
}

func TestRecordShiftCommitment_TheSameDriftAgainIsNotRaised(t *testing.T) {
	f := newDriftFixture()
	f.plan(t, 7)
	f.commitHeads(t, 10)
	f.commitHeads(t, 10)
	f.wantDrifts(t, 1)
	f.wantRecorded(t, 3)
}

func TestRecordShiftCommitment_ADifferentDriftIsRaised(t *testing.T) {
	f := newDriftFixture()
	f.plan(t, 7)
	f.commitHeads(t, 10)
	f.commitHeads(t, 5)
	f.wantDrifts(t, 2)
	if f.drifts[1].DriftHeads != -2 || f.drifts[1].CommittedHeads != 5 {
		t.Fatalf("second drift = %+v, want -2 (5 committed)", f.drifts[1])
	}
	f.wantRecorded(t, -2)
}

func TestRecordShiftCommitment_AClearedDriftThatReturnsIsRaisedAgain(t *testing.T) {
	f := newDriftFixture()
	f.plan(t, 7)
	f.commitHeads(t, 10)
	f.commitHeads(t, 7)
	f.wantDrifts(t, 1)
	f.wantRecorded(t, 0)
	f.commitHeads(t, 10)
	f.wantDrifts(t, 2)
	f.wantRecorded(t, 3)
}

func TestRecordShiftCommitment_NoDriftIsRecordedAsZeroAndNotRaised(t *testing.T) {
	f := newDriftFixture()
	f.plan(t, 7)
	f.commitHeads(t, 7)
	f.wantDrifts(t, 0)
	f.wantRecorded(t, 0)
}

func TestRecordShiftCommitment_RejectsAnInvalidCommitmentBeforeAnyWrite(t *testing.T) {
	for name, tc := range map[string]struct {
		mutate func(*RecordShiftCommitmentCommand)
		want   error
	}{
		"blank site":         {func(c *RecordShiftCommitmentCommand) { c.SiteID = " " }, labourplan.ErrRequiredField},
		"blank shift":        {func(c *RecordShiftCommitmentCommand) { c.ShiftID = "" }, labourplan.ErrRequiredField},
		"blank path":         {func(c *RecordShiftCommitmentCommand) { c.PathID = "" }, labourplan.ErrRequiredField},
		"negative heads":     {func(c *RecordShiftCommitmentCommand) { c.CommittedHeads = -1 }, labourplan.ErrInvalidCount},
		"heads above MaxInt": {func(c *RecordShiftCommitmentCommand) { c.CommittedHeads = labourplan.MaxCount + 1 }, labourplan.ErrInvalidCount},
	} {
		t.Run(name, func(t *testing.T) {
			f := newDriftFixture()
			f.plan(t, 7)
			cmd := commitmentCmd(10)
			tc.mutate(&cmd)
			err := f.record.Handle(context.Background(), cmd)
			if !errors.Is(err, tc.want) || !IsCommitmentRejection(err) {
				t.Fatalf("err = %v, want %v (a rejection)", err, tc.want)
			}
			if _, found := f.stored(t); found {
				t.Fatal("a rejected commitment was stored")
			}
			f.wantDrifts(t, 0)
		})
	}
}

func TestRecordShiftCommitment_TheBoundsAreAccepted(t *testing.T) {
	for _, n := range []int{0, labourplan.MaxCount} {
		f := newDriftFixture()
		f.commitHeads(t, n)
		if c, found := f.stored(t); !found || c.Heads != n {
			t.Fatalf("stored = %+v (found %v), want %d heads", c, found, n)
		}
	}
}

func TestIsCommitmentRejection_InfrastructureIsNotARejection(t *testing.T) {
	if IsCommitmentRejection(errLabourBoom) || IsCommitmentRejection(nil) {
		t.Fatal("an infrastructure error (or nil) was read as a rejection")
	}
}

func TestRecordShiftCommitment_AFailureRollsEverythingBack(t *testing.T) {
	for name, wire := range map[string]func(f *driftFixture){
		"get": func(f *driftFixture) {
			f.record.Commitments = failingCommitments{CommittedHeadsRepo: f.commitments, getErr: errLabourBoom}
		},
		"save": func(f *driftFixture) {
			f.record.Commitments = failingCommitments{CommittedHeadsRepo: f.commitments, saveErr: errLabourBoom}
		},
		"record": func(f *driftFixture) {
			f.record.Commitments = failingCommitments{CommittedHeadsRepo: f.commitments, recordErr: errLabourBoom}
		},
		"plan":    func(f *driftFixture) { f.record.Plans = failingPlanStore{f.plans} },
		"encoder": func(f *driftFixture) { f.record.Encoder = fakeEncoder{err: errLabourBoom} },
		"outbox":  func(f *driftFixture) { f.record.Outbox = failingOutbox{err: errLabourBoom} },
	} {
		t.Run(name, func(t *testing.T) {
			f := newDriftFixture()
			f.plan(t, 7)
			wire(f)
			before := len(f.outbox.Messages())
			if err := f.record.Handle(context.Background(), commitmentCmd(10)); !errors.Is(err, errLabourBoom) {
				t.Fatalf("err = %v, want the injected failure", err)
			}
			if _, found := f.stored(t); found {
				t.Fatal("the failed unit of work left the commitment behind")
			}
			if after := len(f.outbox.Messages()); after != before {
				t.Fatalf("outbox %d -> %d: the failed unit of work queued something", before, after)
			}
		})
	}
}

func TestRecordShiftCommitment_DefaultClockStampsTheDrift(t *testing.T) {
	f := newDriftFixture()
	f.record.Now = nil
	f.plan(t, 7)
	start := time.Now().UTC().Add(-time.Second)
	f.commitHeads(t, 10)
	f.wantDrifts(t, 1)
	if at := f.drifts[0].At; at.Before(start) || at.Location() != time.UTC {
		t.Fatalf("drift at %v, want a UTC now", at)
	}
}
