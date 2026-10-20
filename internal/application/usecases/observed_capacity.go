package usecases

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/claudioed/warehouse-planning/internal/application/ports"
	"github.com/claudioed/warehouse-planning/internal/domain/observed"
)

// RecordObservedCompletion adds one fulfillment-execution TaskCompleted event
// to the observed-capacity read model. The processed-event claim and the
// additive bucket write are ONE ports.UnitOfWork.Do: a redelivered CloudEvents
// id adds nothing, and a failure after the claim rolls the claim back so the
// redelivery is counted (docs/adr/0017 decision 8).
type RecordObservedCompletion struct {
	UoW             ports.UnitOfWork
	ProcessedEvents ports.ProcessedEventRepository
	Observed        ports.ObservedCapacityRepository
}

// Handle claims (consumer, eventID) and records c atomically. OutcomeApplied
// means the completion was counted, OutcomeDuplicate that the CloudEvents id
// was already processed. It returns a non-nil error ONLY for infrastructure
// failures.
func (uc *RecordObservedCompletion) Handle(ctx context.Context, consumer, eventID string, c observed.Completion) (RecordOutcome, error) {
	outcome := OutcomeDuplicate
	err := uc.UoW.Do(ctx, func(ctx context.Context) error {
		claimed, err := uc.ProcessedEvents.Claim(ctx, consumer, eventID)
		if err != nil {
			return err
		}
		if !claimed {
			return nil
		}
		if err := uc.Observed.Record(ctx, c); err != nil {
			return err
		}
		outcome = OutcomeApplied
		return nil
	})
	if err != nil {
		return "", err
	}
	return outcome, nil
}

// PlanVariance is the advisory planned-versus-observed read of one capacity
// plan (docs/adr/0017 decision 9). It never changes the plan.
type PlanVariance struct {
	PlanID   string
	Location string
	// Step is the plan's bottleneck step, the process step whose observed
	// task completions are compared.
	Step        string
	WindowStart time.Time
	WindowEnd   time.Time
	// PlannedPerHour is the plan's path capacity (ORDER/hour) and
	// PlannedOverWindow its capacity over the window (orders).
	PlannedPerHour    float64
	PlannedOverWindow float64
	observed.Variance
}

// GetCapacityPlanVariance compares what a stored plan assumed with the task
// throughput observed for its bottleneck step at its location over its window.
type GetCapacityPlanVariance struct {
	Plans    ports.CapacityPlanRepository
	Observed ports.ObservedCapacityRepository
}

// Handle returns the PlanVariance of plan id. ErrCapacityPlanNotFound for an
// unknown id; observed.ErrNonPositiveTasksPerOrder for a bad factor. tasksPerOrder
// nil means the caller supplied none: the observed side is still reported and
// the comparison says why it cannot be computed.
func (uc *GetCapacityPlanVariance) Handle(ctx context.Context, planID string, tasksPerOrder *float64) (PlanVariance, error) {
	plan, err := uc.Plans.FindByID(ctx, planID)
	if err != nil {
		return PlanVariance{}, err
	}
	if plan == nil {
		return PlanVariance{}, ErrCapacityPlanNotFound
	}
	window := plan.Window()
	step := string(plan.BottleneckStep())
	// A task_type is upper-case; a step registered as "pick" is the same step.
	throughput, err := uc.Observed.Throughput(ctx, plan.Location(), strings.ToUpper(step), window.Start(), window.End())
	if err != nil {
		return PlanVariance{}, fmt.Errorf("read observed throughput: %w", err)
	}
	variance, err := observed.Compare(plan.CapacityOverWindow(), window.Duration(), throughput, tasksPerOrder)
	if err != nil {
		return PlanVariance{}, err
	}
	return PlanVariance{
		PlanID: plan.ID(), Location: plan.Location(), Step: step,
		WindowStart: window.Start(), WindowEnd: window.End(),
		PlannedPerHour: plan.PathCapacity(), PlannedOverWindow: plan.CapacityOverWindow(),
		Variance: variance,
	}, nil
}
