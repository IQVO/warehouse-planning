package usecases

import (
	"context"
	"time"

	"github.com/google/uuid"

	"github.com/claudioed/warehouse-planning/internal/application/ports"
	"github.com/claudioed/warehouse-planning/internal/domain/capacityplan"
	"github.com/claudioed/warehouse-planning/internal/domain/processcapacity"
)

// CreateCapacityPlanCommand carries everything needed to evaluate one
// ProcessPath against the demand assigned to one location and window.
//
// AssignedDemand (orders) arrives on the request. PHASE 4 SIMPLIFICATION:
// the final demand-ingestion shape from order-management /
// network-fulfillment is a later decision, and CLAUDE.md's cross-context
// rule forbids a live lookup, so the caller states the demand directly.
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
	WarehouseID   string
	Location      string
	WindowStart   time.Time
	WindowEnd     time.Time
	ProcessPathID string

	AssignedDemand float64

	UnitsPerOrder    *float64
	PackagesPerOrder *float64
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

	// NewID mints the plan id (uuid.NewString when nil) and Now supplies
	// the creation time (utcNow when nil); both are injectable
	// for tests.
	NewID func() string
	Now   func() time.Time
}

// Handle creates and persists a DRAFT CapacityPlan. Errors: the window
// validation error (processcapacity.ErrInvalidWindow),
// ErrProcessPathNotFound, any ComputeProcessPathCapacity error
// (missing step capacity, bad/missing conversion factor), and the
// aggregate's validation errors (capacityplan.ErrNegativeDemand,
// ErrRequiredField).
func (uc *CreateCapacityPlan) Handle(ctx context.Context, cmd CreateCapacityPlanCommand) (*capacityplan.CapacityPlan, error) {
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

	plan, err := capacityplan.Create(capacityplan.CreateParams{
		ID:             newID(),
		WarehouseID:    cmd.WarehouseID,
		Location:       cmd.Location,
		Window:         window,
		ProcessPathID:  cmd.ProcessPathID,
		AssignedDemand: cmd.AssignedDemand,
		PathRate:       result.NormalizedRate,
		BottleneckStep: result.BottleneckStep,

		BottleneckConstraint: result.BottleneckConstraint,
		Warnings:             result.Warnings,
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

// utcNow is the default clock of the plan use cases: the current time in
// UTC, truncated to microseconds -- Postgres TIMESTAMPTZ's precision -- so a
// plan reloaded from the database carries exactly the timestamps the
// request and the published events saw.
func utcNow() time.Time { return time.Now().UTC().Truncate(time.Microsecond) }

// enqueue encodes events and inserts them into the outbox through ctx (the
// caller's UnitOfWork transaction).
func enqueue(ctx context.Context, enc ports.EventEncoder, out ports.OutboxRepository, events []capacityplan.Event) error {
	msgs, err := enc.Encode(events...)
	if err != nil {
		return err
	}
	return out.Insert(ctx, msgs...)
}
