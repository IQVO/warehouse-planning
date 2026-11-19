package usecases

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"

	"github.com/claudioed/warehouse-planning/internal/application/ports"
	"github.com/claudioed/warehouse-planning/internal/domain/capacityplan"
	"github.com/claudioed/warehouse-planning/internal/domain/processcapacity"
)

// CreateCapacityPlanCommand carries everything needed to evaluate one
// ProcessPath against the demand assigned to one location and window.
//
// AssignedDemand (orders) arrives on the request. It is OPTIONAL
// (docs/adr/0004): an explicit value always wins and behaves exactly as
// the original Phase 2 shape did; when omitted (DemandFromOrders) the
// demand comes from the expected-demand read model fed by
// order-management's published events instead (see Demand/resolveDemand
// below) -- the "final demand-ingestion shape is a later decision" open
// question this comment used to record is resolved.
//
// UnitsPerOrder/PackagesPerOrder are the WorkloadProfile factors, passed
// per request exactly as in Phase 2's path-capacity endpoint (no
// WorkloadProfile persistence yet).
//
// The window is the plan's PlanningWindow AND the window every step's
// ProcessCapacity is looked up for: a registered window applies when it
// COVERS [WindowStart, WindowEnd) (docs/adr/0003), exactly as in
// GetProcessPathCapacity. The plan stores this requested window as is.
type CreateCapacityPlanCommand struct {
	WarehouseID string
	// SiteID is the canonical site (a facility-layout Site site_code) the
	// plan is scoped to. REQUIRED: a blank one is rejected with
	// capacityplan.ErrRequiredField; it is never defaulted from
	// WarehouseID or Location (the plan's Location is a planning
	// location, not a site code).
	SiteID        string
	Location      string
	WindowStart   time.Time
	WindowEnd     time.Time
	ProcessPathID string

	AssignedDemand float64

	// DemandFromOrders is set by the adapters when the request OMITTED
	// assigned_demand (AssignedDemand is then ignored): the demand is the
	// number of orders the read model expects at Location in the window
	// (docs/adr/0004). An explicit assigned_demand NEVER sets it, so a
	// stated figure always wins and behaves exactly as before.
	DemandFromOrders bool

	// DemandSource is the OPTIONAL demand source the request named
	// (docs/adr/0024 decision 1): "request", "orders", "forecast" or empty
	// for the default (request when assigned_demand is stated, orders when it
	// is omitted). An input the named source does not read is refused
	// (capacityplan.ErrDemandSourceConflict); see requestedSource.
	DemandSource string
	// ForecastPathID names the WorkloadForecast a forecast demand is read
	// from; empty is the plan's ProcessPathID. Both are canonicalised
	// (workloadforecast.CanonicalPathID) before the lookup.
	ForecastPathID string

	UnitsPerOrder    *float64
	PackagesPerOrder *float64

	// TasksPerOrder is the OPTIONAL tasks-per-order factor per process type
	// (docs/adr/0022 decision 5), next to the WorkloadProfile factors: a step's
	// tasks are AssignedDemand x its factor (PICK with demand from orders uses
	// the released lines instead). Every factor must be positive
	// (capacityplan.ErrNonPositiveTasksPerOrder); a step without one has an
	// unknown labour requirement, never a guessed one.
	TasksPerOrder map[string]float64
}

// CreateCapacityPlan computes the path capacity (reusing
// GetProcessPathCapacity, i.e. the shared read-time composition), builds the CapacityPlan aggregate, saves it and stores its
// CapacityPlanCreated event in the outbox -- the save and the outbox
// insert are ONE ports.UnitOfWork.Do, so they commit or roll back together.
type CreateCapacityPlan struct {
	PathCapacity *GetProcessPathCapacity
	Plans        ports.CapacityPlanRepository
	Outbox       ports.OutboxRepository
	Encoder      ports.EventEncoder
	UnitOfWork   ports.UnitOfWork

	// Demand is the expected-demand read (docs/adr/0004). Nil means no
	// demand model is wired: an omitted assigned_demand then fails with
	// ErrMissingAssignedDemand exactly as before the model existed.
	Demand *GetExpectedDemand

	// Forecasts is the WorkloadForecast store (docs/adr/0023) a
	// demand_source forecast reads (docs/adr/0024). Nil means no forecast is
	// wired: a forecast demand then fails with ErrMissingAssignedDemand.
	Forecasts ports.WorkloadForecastRepository

	// Inbound is the dock-appointment demand read (docs/adr/0017): the
	// appointments starting in the window priced with the declared inbound
	// labour standard, stored on the plan NEXT TO (never summed into) the order
	// demand. Nil means no inbound model is wired and the plan carries no
	// inbound demand, exactly as before the model existed.
	Inbound *GetInboundDemand

	// LaborStandards is the local copy of labor-performance's engineered
	// labour standards (docs/adr/0022): each path step is priced with the
	// standard of its task type in force for the plan's site at the window
	// start. Nil means no copy is wired: every step's labour requirement is
	// unknown (no-labor-standard) and everything else is unchanged.
	LaborStandards ports.LaborStandardRepository

	// NewID mints the plan id (uuid.NewString when nil) and Now supplies
	// the creation time (utcNow when nil); both are injectable
	// for tests.
	NewID func() string
	Now   func() time.Time

	// Metrics records the Tier-2 business counter
	// warehouse_planning.capacity_plans.created (fleet standard-metrics
	// ADR, docs/adr/0011). Nil is a documented no-op (ports.PlanMetrics
	// implementations are nil-safe), so a caller that predates this
	// field behaves exactly as before.
	Metrics ports.PlanMetrics
}

// Handle creates and persists a DRAFT CapacityPlan. Errors: the window
// validation error (processcapacity.ErrInvalidWindow),
// ErrProcessPathNotFound, any ComputeProcessPathCapacity error
// (missing step capacity, bad/missing conversion factor), the
// aggregate's validation errors (capacityplan.ErrNegativeDemand,
// ErrRequiredField) and ErrMissingAssignedDemand (demand omitted and no
// orders expected in the window).
func (uc *CreateCapacityPlan) Handle(ctx context.Context, cmd CreateCapacityPlanCommand) (*capacityplan.CapacityPlan, error) {
	plan, err := uc.handle(ctx, cmd)
	// uc.Metrics is an interface field: a caller that predates it (every
	// pre-existing test) leaves it as the nil interface, not a nil
	// *telemetry.PlanMetrics, so the nil check MUST happen here rather
	// than relying solely on the implementation's own nil-receiver
	// safety -- calling a method on a nil interface panics.
	if uc.Metrics != nil {
		if err != nil {
			uc.Metrics.CapacityPlanCreated(ctx, ports.PlanOutcomeRejected)
		} else {
			uc.Metrics.CapacityPlanCreated(ctx, ports.PlanOutcomeCreated)
		}
	}
	if err != nil {
		return nil, err
	}
	return plan, nil
}

func (uc *CreateCapacityPlan) handle(ctx context.Context, cmd CreateCapacityPlanCommand) (*capacityplan.CapacityPlan, error) {
	// A blank identifying field is refused before anything is looked up or
	// derived, so it is never reported as a missing step capacity, an unknown
	// path or a missing demand (openapi, ADR 0012, troubleshooting:
	// 422 missing-required-field).
	if err := capacityplan.ValidateRequired(cmd.WarehouseID, cmd.SiteID, cmd.Location, cmd.ProcessPathID); err != nil {
		return nil, err
	}
	factors := taskFactors(cmd.TasksPerOrder)
	if err := capacityplan.ValidateTasksPerOrder(factors); err != nil {
		return nil, err
	}

	// Resolve the demand next: an omitted assigned_demand with no order
	// data has always been rejected before any other check.
	demand, err := uc.resolveDemand(ctx, cmd)
	if err != nil {
		return nil, err
	}
	demand.TasksPerOrder = factors

	window, err := processcapacity.NewCapacityWindow(cmd.WindowStart, cmd.WindowEnd)
	if err != nil {
		return nil, err
	}

	result, err := uc.PathCapacity.Handle(ctx, GetProcessPathCapacityCommand{
		ProcessPathID:    cmd.ProcessPathID,
		Location:         cmd.Location,
		WindowStart:      cmd.WindowStart,
		WindowEnd:        cmd.WindowEnd,
		UnitsPerOrder:    cmd.UnitsPerOrder,
		PackagesPerOrder: cmd.PackagesPerOrder,
	})
	if err != nil {
		return nil, err
	}

	newID, now := uc.NewID, uc.Now
	if newID == nil {
		newID = uuid.NewString
	}
	if now == nil {
		now = utcNow
	}

	inboundDemand, inboundWarnings, err := uc.resolveInbound(ctx, cmd)
	if err != nil {
		return nil, err
	}
	labour, err := uc.resolveLabour(ctx, cmd.SiteID, window, demand, result.Steps)
	if err != nil {
		return nil, err
	}

	plan, err := capacityplan.Create(capacityplan.CreateParams{
		ID:             newID(),
		WarehouseID:    cmd.WarehouseID,
		SiteID:         cmd.SiteID,
		Location:       cmd.Location,
		Window:         window,
		ProcessPathID:  cmd.ProcessPathID,
		AssignedDemand: demand.AssignedDemand,
		DemandSource:   demand.Source,
		ForecastPathID: forecastPathOf(cmd),
		PathRate:       result.NormalizedRate,
		BottleneckStep: result.BottleneckStep,

		BottleneckConstraint: result.BottleneckConstraint,
		Warnings:             append(append([]string(nil), result.Warnings...), inboundWarnings...),
		Inbound:              inboundDemand,
		LabourRequirement:    labour,
	}, now())
	if err != nil {
		return nil, err
	}

	err = uc.UnitOfWork.Do(ctx, func(ctx context.Context) error {
		if err := uc.Plans.Save(ctx, plan); err != nil {
			return err
		}
		return enqueue(ctx, uc.Encoder, uc.Outbox, plan.PullEvents())
	})
	if err != nil {
		return nil, err
	}
	return plan, nil
}

// resolveDemand returns the demand the plan is evaluated against and where it
// came from, by the source requestedSource settles on. A stated
// assigned_demand (source request) is used untouched; the forecast source is
// forecastDemand. Otherwise it is
// the number of orders expected at the plan's location in its window, with
// the released lines of those orders (the PICK tasks, docs/adr/0022); no
// orders (or no model) is ErrMissingAssignedDemand -- never a silent zero.
func (uc *CreateCapacityPlan) resolveDemand(ctx context.Context, cmd CreateCapacityPlanCommand) (capacityplan.LabourDemand, error) {
	source, err := requestedSource(cmd)
	if err != nil {
		return capacityplan.LabourDemand{}, err
	}
	switch source {
	case capacityplan.DemandSourceRequest:
		return capacityplan.LabourDemand{AssignedDemand: cmd.AssignedDemand, Source: capacityplan.DemandSourceRequest}, nil
	case capacityplan.DemandSourceForecast:
		return uc.forecastDemand(ctx, cmd)
	}
	if uc.Demand == nil {
		return capacityplan.LabourDemand{}, ErrMissingAssignedDemand
	}
	summary, err := uc.Demand.Handle(ctx, cmd.Location, cmd.WindowStart, cmd.WindowEnd)
	if err != nil {
		// An unusable window cannot have demand; the request is rejected
		// the way an omitted assigned_demand always was.
		if errors.Is(err, processcapacity.ErrInvalidWindow) {
			return capacityplan.LabourDemand{}, ErrMissingAssignedDemand
		}
		return capacityplan.LabourDemand{}, err
	}
	if !summary.HasData() {
		return capacityplan.LabourDemand{}, ErrMissingAssignedDemand
	}
	return capacityplan.LabourDemand{
		AssignedDemand: float64(summary.Orders),
		Source:         capacityplan.DemandSourceOrders,
		ReleasedLines:  summary.ReleasedLines,
	}, nil
}

// resolveInbound reads the inbound demand of the plan's (location, path,
// window) and returns it with the warning the plan carries when appointments
// exist and the site declared no inbound standard for ANY path (the "inbound
// standard missing" of docs/adr/0017 decision 6). No inbound model wired is
// no inbound demand and no warning.
func (uc *CreateCapacityPlan) resolveInbound(ctx context.Context, cmd CreateCapacityPlanCommand) (capacityplan.InboundDemand, []string, error) {
	if uc.Inbound == nil {
		return capacityplan.InboundDemand{}, nil, nil
	}
	res, err := uc.Inbound.Handle(ctx, cmd.Location, cmd.ProcessPathID, cmd.WindowStart, cmd.WindowEnd)
	if err != nil {
		return capacityplan.InboundDemand{}, nil, err
	}
	demand := capacityplan.InboundDemand{
		Appointments:    res.Demand.Appointments,
		LabourMinutes:   res.LabourMinutes,
		StandardMissing: res.StandardMissing,
	}
	var warnings []string
	if res.StandardMissing && res.SiteHasNoStandard {
		warnings = append(warnings, fmt.Sprintf(
			"inbound standard missing: %d dock appointment(s) at %s start in this window and contribute no labour demand; declare minutes per appointment with PUT /inbound-labour-standards/%s/%s",
			res.Demand.Appointments, cmd.Location, cmd.Location, cmd.ProcessPathID))
	}
	return demand, warnings, nil
}

// utcNow is the default clock of the plan use cases: the current time in
// UTC, truncated to microseconds -- Postgres TIMESTAMPTZ's precision -- so a
// plan reloaded from the database carries exactly the timestamps the
// request and the published events saw.
func utcNow() time.Time { return time.Now().UTC().Truncate(time.Microsecond) }

// enqueue encodes CapacityPlan events and inserts them into the outbox
// through ctx (the caller's UnitOfWork transaction).
func enqueue(ctx context.Context, enc ports.EventEncoder, out ports.OutboxRepository, events []capacityplan.Event) error {
	msgs, err := enc.EncodeCapacityPlanEvents(events...)
	if err != nil {
		return err
	}
	return out.Insert(ctx, msgs...)
}
