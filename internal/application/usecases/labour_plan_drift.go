package usecases

import (
	"context"
	"errors"
	"strings"
	"time"

	"github.com/claudioed/warehouse-planning/internal/application/ports"
	"github.com/claudioed/warehouse-planning/internal/domain/labourplan"
)

// RecordShiftCommitmentCommand is one line of workforce-management's
// ShiftPlanCommitted as the labour plan drift reads it: the heads committed
// for (site, shift, path) and when the commitment was observed (the event
// time).
type RecordShiftCommitmentCommand struct {
	SiteID         string
	ShiftID        string
	PathID         string
	CommittedHeads int
	ObservedAt     time.Time
}

// RecordShiftCommitment is the commitment trigger of the labour plan drift
// (docs/adr/0023 decision 3, wes-work-planning ADR 0019's surfacing rule
// kept): it upserts the committed heads of the key and, when a labour plan
// exists for it, compares the two, inserts a LabourPlanDriftDetected into the
// outbox when the drift is news (labourplan.Drift.ShouldRaise) and records
// the drift, zero included -- all in ONE ports.UnitOfWork.Do, which joins the
// labor consumer's open unit of work (its processed-event claim and LABOR
// registration). It never changes the labour plan.
type RecordShiftCommitment struct {
	Commitments ports.CommittedHeadsRepository
	Plans       ports.LabourPlanRepository
	Outbox      ports.OutboxRepository
	Encoder     ports.EventEncoder
	UnitOfWork  ports.UnitOfWork

	// Now supplies the detection time of a raised drift (utcNow when nil).
	Now func() time.Time
}

// Handle records the commitment. Errors: labourplan.ErrRequiredField (a blank
// site, shift or path id) and labourplan.ErrInvalidCount (heads below zero or
// above labourplan.MaxCount), both before anything is read or written (see
// IsCommitmentRejection), and any store, encoder or outbox failure.
func (uc *RecordShiftCommitment) Handle(ctx context.Context, cmd RecordShiftCommitmentCommand) error {
	if strings.TrimSpace(cmd.SiteID) == "" || strings.TrimSpace(cmd.ShiftID) == "" || strings.TrimSpace(cmd.PathID) == "" {
		return labourplan.ErrRequiredField
	}
	if cmd.CommittedHeads < 0 || cmd.CommittedHeads > labourplan.MaxCount {
		return labourplan.ErrInvalidCount
	}
	now := uc.Now
	if now == nil {
		now = utcNow
	}
	return uc.UnitOfWork.Do(ctx, func(ctx context.Context) error {
		stored, _, err := uc.Commitments.Get(ctx, cmd.SiteID, cmd.ShiftID, cmd.PathID)
		if err != nil {
			return err
		}
		c := labourplan.Commitment{
			SiteID: cmd.SiteID, ShiftID: cmd.ShiftID, PathID: cmd.PathID,
			Heads: cmd.CommittedHeads, ObservedAt: cmd.ObservedAt,
			LastDriftHeads: stored.LastDriftHeads, HasLastDrift: stored.HasLastDrift,
		}
		if err := uc.Commitments.SaveCommitment(ctx, c); err != nil {
			return err
		}
		plan, found, err := uc.Plans.Get(ctx, cmd.SiteID, cmd.ShiftID, cmd.PathID)
		if err != nil || !found {
			return err
		}
		return driftRecorder{uc.Commitments, uc.Outbox, uc.Encoder}.record(ctx, plan.Spec().PlannedHeads, c, now())
	})
}

// IsCommitmentRejection reports whether err (as returned by
// RecordShiftCommitment.Handle) is a deterministic rejection of the command,
// which retrying can never fix, as opposed to an infrastructure failure.
func IsCommitmentRejection(err error) bool {
	return errors.Is(err, labourplan.ErrRequiredField) || errors.Is(err, labourplan.ErrInvalidCount)
}

// surfaceDrift is the plan trigger of the labour plan drift (docs/adr/0023
// decision 3): called by commit only when the plan CHANGED, on the unit of
// work of the plan's save, it compares the plan's heads with the heads
// workforce-management committed for the key, raises LabourPlanDriftDetected
// when the drift is news and records it. No commitment stored, or no
// Commitments wired, surfaces nothing. It never changes the commitment's heads.
func (uc *CommitLabourPlan) surfaceDrift(ctx context.Context, s labourplan.Spec, at time.Time) error {
	if uc.Commitments == nil {
		return nil
	}
	c, found, err := uc.Commitments.Get(ctx, s.SiteID, s.ShiftID, s.PathID)
	if err != nil || !found {
		return err
	}
	return driftRecorder{uc.Commitments, uc.Outbox, uc.Encoder}.record(ctx, s.PlannedHeads, c, at)
}

// driftRecorder is the raise rule both drift triggers share (docs/adr/0023
// decision 3): compare, raise when the drift is news, record the drift.
type driftRecorder struct {
	commitments ports.CommittedHeadsRepository
	outbox      ports.OutboxRepository
	encoder     ports.EventEncoder
}

// record compares plannedHeads with the commitment c, inserts the
// LabourPlanDriftDetected of the drift into the outbox when it is non-zero
// and differs from c's last recorded drift, and records the drift (zero
// included, so a drift that clears and returns is raised again). It runs on
// the caller's unit of work and changes neither side.
func (r driftRecorder) record(ctx context.Context, plannedHeads int, c labourplan.Commitment, at time.Time) error {
	d := labourplan.CompareHeads(plannedHeads, c.Heads)
	if d.ShouldRaise(c.LastDriftHeads, c.HasLastDrift) {
		msgs, err := r.encoder.EncodeLabourPlanDriftDetected(d.Event(c.SiteID, c.ShiftID, c.PathID, at))
		if err != nil {
			return err
		}
		if err := r.outbox.Insert(ctx, msgs...); err != nil {
			return err
		}
	}
	return r.commitments.RecordDrift(ctx, c.SiteID, c.ShiftID, c.PathID, d.Heads)
}
