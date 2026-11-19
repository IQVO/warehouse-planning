package usecases

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/claudioed/warehouse-planning/internal/application/ports"
	"github.com/claudioed/warehouse-planning/internal/domain/labourplan"
)

var (
	// ErrMissingPlannedHeads is returned when planned_heads was omitted and
	// cannot be derived (docs/adr/0023 decision 2): there is no shift_start,
	// no stored workload forecast for (site, path), or no forecast bucket
	// with a cpt in [shift_start, shift_start + hours). Zero buckets in the
	// shift is no data, never zero heads.
	ErrMissingPlannedHeads = errors.New("usecases: planned_heads was omitted and cannot be derived: it needs shift_start and a stored workload forecast with at least one bucket in [shift_start, shift_start + hours)")

	// ErrLabourPlanNotFound is returned when no LabourPlan exists for the
	// requested (site id, shift id, path id).
	ErrLabourPlanNotFound = errors.New("usecases: no labour plan exists for this site, shift and path")
)

// CommitLabourPlanCommand is one commit of the heads a process path needs for
// one shift at one site (docs/adr/0023 decision 2). PlannedHeads nil means
// "derive them from the stored workload forecast", which needs ShiftStart.
type CommitLabourPlanCommand struct {
	SiteID            string
	ShiftID           string
	PathID            string
	PlannedHeads      *int
	RateUnitsPerHour  float64
	Hours             float64
	InstalledStations int
	ShiftStart        *time.Time
}

// LabourPlanView is a stored plan with the advisory warnings of its read:
// today only the station-tally one (see stationWarnings). Warnings are never
// stored; they are computed against the tally on every read.
type LabourPlanView struct {
	Plan     *labourplan.Plan
	Warnings []string
}

// CommitLabourPlanResult is the plan after the commit. Created is true for a
// first commit (version 1); Changed whenever something was stored. A commit
// equal to the stored plan is Created=false, Changed=false: nothing was
// written, no event was queued and the version stayed.
type CommitLabourPlanResult struct {
	LabourPlanView
	Created bool
	Changed bool
}

// CommitLabourPlan stores a commit of the LabourPlan aggregate: it builds the
// labourplan.Spec from the command (deriving planned_heads from the stored
// workload forecast when they were omitted), loads the plan of (site, shift,
// path), builds it with labourplan.New when absent or applies Revise
// otherwise and, when that changed something, saves it, inserts its
// LabourPlanCommitted CloudEvents into the outbox and surfaces the drift
// against the committed heads (see surfaceDrift) -- all in ONE
// ports.UnitOfWork.Do. A ports.ErrVersionConflict re-runs the whole unit
// once; a second one is returned as is.
type CommitLabourPlan struct {
	Plans      ports.LabourPlanRepository
	Forecasts  ports.WorkloadForecastRepository
	Outbox     ports.OutboxRepository
	Encoder    ports.EventEncoder
	UnitOfWork ports.UnitOfWork

	// Stations is this context's facility station tally (ADR 0002), the
	// reader the capacity plan's station constraint uses. Nil means no tally
	// is wired: no station warning is ever raised.
	Stations ports.StorageTallyReader

	// Commitments is the heads workforce-management committed per key, the
	// other side of the labour plan drift (docs/adr/0023 decision 3). Nil
	// means no drift is ever surfaced by a plan commit.
	Commitments ports.CommittedHeadsRepository

	// Now supplies the commit time (utcNow when nil).
	Now func() time.Time
}

// Handle commits the plan. Errors: ErrMissingPlannedHeads, the aggregate's
// validation errors (labourplan.ErrRequiredField, ErrInvalidCount,
// ErrHeadsExceedStations, ErrInvalidRate, ErrInvalidHours,
// ErrThroughputNotFinite), ports.ErrVersionConflict after the one retry, and
// any store, tally, encoder or outbox failure.
func (uc *CommitLabourPlan) Handle(ctx context.Context, cmd CommitLabourPlanCommand) (CommitLabourPlanResult, error) {
	now := uc.Now
	if now == nil {
		now = utcNow
	}
	var res CommitLabourPlanResult
	err := retryOnVersionConflict(func() error {
		var err error
		res, err = uc.commit(ctx, cmd, now())
		return err
	})
	if err != nil {
		return CommitLabourPlanResult{}, err
	}
	return res, nil
}

// commit is one attempt: the forecast read, the plan load, the decision, the
// tally read and the save in one unit of work, so a failure anywhere stores
// nothing.
func (uc *CommitLabourPlan) commit(ctx context.Context, cmd CommitLabourPlanCommand, at time.Time) (CommitLabourPlanResult, error) {
	var res CommitLabourPlanResult
	err := uc.UnitOfWork.Do(ctx, func(ctx context.Context) error {
		spec, err := uc.spec(ctx, cmd)
		if err != nil {
			return err
		}
		stored, found, err := uc.Plans.Get(ctx, cmd.SiteID, cmd.ShiftID, cmd.PathID)
		if err != nil {
			return err
		}
		var event labourplan.Committed
		if found {
			res.Plan = stored
			event, res.Changed, err = stored.Revise(spec, at)
		} else {
			res.Plan, event, err = labourplan.New(spec, at)
			res.Created, res.Changed = true, true
		}
		if err != nil {
			return err
		}
		if res.Warnings, err = stationWarnings(ctx, uc.Stations, spec); err != nil || !res.Changed {
			return err
		}
		if err := uc.Plans.Save(ctx, res.Plan); err != nil {
			return err
		}
		msgs, err := uc.Encoder.EncodeLabourPlanCommitted(event)
		if err != nil {
			return err
		}
		if err := uc.Outbox.Insert(ctx, msgs...); err != nil {
			return err
		}
		return uc.surfaceDrift(ctx, res.Plan.Spec(), at)
	})
	return res, err
}

// spec builds the plan content. Sent heads are used as sent
// (heads_source request); omitted ones are derived (heads_source forecast).
func (uc *CommitLabourPlan) spec(ctx context.Context, cmd CommitLabourPlanCommand) (labourplan.Spec, error) {
	s := labourplan.Spec{
		SiteID:            cmd.SiteID,
		ShiftID:           cmd.ShiftID,
		PathID:            cmd.PathID,
		RateUnitsPerHour:  cmd.RateUnitsPerHour,
		Hours:             cmd.Hours,
		InstalledStations: cmd.InstalledStations,
		ShiftStart:        cmd.ShiftStart,
		HeadsSource:       labourplan.HeadsFromRequest,
	}
	if cmd.PlannedHeads != nil {
		s.PlannedHeads = *cmd.PlannedHeads
		return s, nil
	}
	heads, err := uc.deriveHeads(ctx, cmd)
	if err != nil {
		return labourplan.Spec{}, err
	}
	s.PlannedHeads, s.HeadsSource = heads, labourplan.HeadsFromForecast
	return s, nil
}

// deriveHeads is docs/adr/0023 decision 2: ceil(forecast units with a cpt in
// [shift_start, shift_start + hours) / (rate x hours)). The rate and hours
// rules are checked first, so an unusable hours is reported as itself and
// never as a shift without forecast data.
func (uc *CommitLabourPlan) deriveHeads(ctx context.Context, cmd CommitLabourPlanCommand) (int, error) {
	if _, err := labourplan.DeriveHeads(0, cmd.RateUnitsPerHour, cmd.Hours); err != nil {
		return 0, err
	}
	if cmd.ShiftStart == nil || uc.Forecasts == nil {
		return 0, ErrMissingPlannedHeads
	}
	forecast, found, err := uc.Forecasts.Get(ctx, cmd.SiteID, cmd.PathID)
	if err != nil {
		return 0, err
	}
	if !found {
		return 0, ErrMissingPlannedHeads
	}
	units, buckets := forecast.QuantityDueIn(*cmd.ShiftStart, labourplan.ShiftEnd(*cmd.ShiftStart, cmd.Hours))
	if buckets == 0 {
		return 0, ErrMissingPlannedHeads
	}
	return labourplan.DeriveHeads(units, cmd.RateUnitsPerHour, cmd.Hours)
}

// GetLabourPlan reads the stored plan of (site, shift, path) with its station
// warning. A pure read.
type GetLabourPlan struct {
	Plans ports.LabourPlanRepository
	// Stations: see CommitLabourPlan.Stations.
	Stations ports.StorageTallyReader
}

// Handle returns the plan, or ErrLabourPlanNotFound.
func (uc *GetLabourPlan) Handle(ctx context.Context, siteID, shiftID, pathID string) (LabourPlanView, error) {
	p, found, err := uc.Plans.Get(ctx, siteID, shiftID, pathID)
	if err != nil {
		return LabourPlanView{}, err
	}
	if !found {
		return LabourPlanView{}, ErrLabourPlanNotFound
	}
	warnings, err := stationWarnings(ctx, uc.Stations, p.Spec())
	if err != nil {
		return LabourPlanView{}, err
	}
	return LabourPlanView{Plan: p, Warnings: warnings}, nil
}

// ListLabourPlans lists the plans of one (site, shift), ordered by path id,
// one keyset page at a time, each with its station warning. A pure read.
type ListLabourPlans struct {
	Plans ports.LabourPlanRepository
	// Stations: see CommitLabourPlan.Stations.
	Stations ports.StorageTallyReader
}

// Handle lists up to limit plans whose path id sorts after afterPathID ("" =
// the first page). The caller owns the limit bounds (the inbound adapter's
// page package); the result is never nil.
func (uc *ListLabourPlans) Handle(ctx context.Context, siteID, shiftID, afterPathID string, limit int) ([]LabourPlanView, error) {
	plans, _, err := uc.Plans.List(ctx, siteID, shiftID, afterPathID, limit)
	if err != nil {
		return nil, err
	}
	out := make([]LabourPlanView, 0, len(plans))
	for _, p := range plans {
		warnings, err := stationWarnings(ctx, uc.Stations, p.Spec())
		if err != nil {
			return nil, err
		}
		out = append(out, LabourPlanView{Plan: p, Warnings: warnings})
	}
	return out, nil
}

// stationWarnings compares the caller-supplied installed_stations with the
// facility tally (docs/adr/0023 decision 2, ADR 0002): when the tally of the
// site knows FEWER stations of the activity named like the path (the path id
// upper-cased, as the capacity plan's station constraint reads it) the plan
// carries a warning. The number is reported as sent, never replaced. No tally
// wired is no warning.
func stationWarnings(ctx context.Context, tally ports.StorageTallyReader, s labourplan.Spec) ([]string, error) {
	if tally == nil {
		return nil, nil
	}
	activity := strings.ToUpper(s.PathID)
	known, err := tally.StationCount(ctx, s.SiteID, activity)
	if err != nil {
		return nil, err
	}
	// Zero tallied stations is no tally data (the capacity plan reads it the
	// same way: station standards apply only when the count is above zero), so
	// it never warns; only a tally that KNOWS fewer stations does.
	if known == 0 || known >= s.InstalledStations {
		return nil, nil
	}
	return []string{fmt.Sprintf(
		"installed stations above the facility tally: installed_stations is %d, the facility-layout tally knows %d %s station(s) at %s; the plan keeps %d as sent",
		s.InstalledStations, known, activity, s.SiteID, s.InstalledStations)}, nil
}
