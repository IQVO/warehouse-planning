package usecases

import (
	"context"
	"errors"
	"time"

	"github.com/claudioed/warehouse-planning/internal/application/ports"
	"github.com/claudioed/warehouse-planning/internal/domain/demand"
	"github.com/claudioed/warehouse-planning/internal/domain/processcapacity"
)

// ErrMissingAssignedDemand is returned by CreateCapacityPlan when the caller
// omitted assigned_demand AND the expected-demand read model has no orders
// for the plan's location and window (or is not wired at all). The plan is
// never created with a silent zero. The message is the one the REST and MCP
// adapters have always returned for an absent assigned_demand.
var ErrMissingAssignedDemand = errors.New("assigned_demand (orders) must be provided")

// RecordOutcome says what RecordOrderDemand did with one event.
type RecordOutcome string

// The outcomes of RecordOrderDemand.Handle.
const (
	// OutcomeApplied: the order was inserted or replaced.
	OutcomeApplied RecordOutcome = "applied"
	// OutcomeDuplicate: the CloudEvents id was already processed; nothing
	// was written.
	OutcomeDuplicate RecordOutcome = "duplicate"
	// OutcomeStale: a newer write for the same order id is already stored;
	// the event was claimed but changed nothing.
	OutcomeStale RecordOutcome = "stale"
)

// RecordOrderDemand writes one order-management order event into the
// expected-demand read model. The processed-event claim and the upsert are
// ONE ports.UnitOfWork.Do, so a failure after the claim rolls the claim back
// and the redelivery is processed instead of being skipped as already
// handled.
type RecordOrderDemand struct {
	UoW             ports.UnitOfWork
	ProcessedEvents ports.ProcessedEventRepository
	Demand          ports.OrderDemandRepository
}

// Handle claims (consumer, eventID) and upserts o atomically. It returns a
// non-nil error ONLY for infrastructure failures (begin/commit, claim,
// upsert); everything deterministic was rejected before the order was
// built.
func (uc *RecordOrderDemand) Handle(ctx context.Context, consumer, eventID string, o demand.Order) (RecordOutcome, error) {
	outcome := OutcomeDuplicate
	err := uc.UoW.Do(ctx, func(ctx context.Context) error {
		claimed, err := uc.ProcessedEvents.Claim(ctx, consumer, eventID)
		if err != nil {
			return err
		}
		if !claimed {
			return nil
		}
		applied, err := uc.Demand.Upsert(ctx, o)
		if err != nil {
			return err
		}
		outcome = OutcomeStale
		if applied {
			outcome = OutcomeApplied
		}
		return nil
	})
	if err != nil {
		return "", err
	}
	return outcome, nil
}

// GetExpectedDemand reads the expected demand of a site over a window from
// the local read model: how many orders' promise cutoff falls in
// [start, end), the released lines of those orders, and the as-of time of
// the newest event the site's model reflects.
type GetExpectedDemand struct {
	Demand ports.OrderDemandRepository
}

// Handle returns the site's Summary. It fails with
// processcapacity.ErrInvalidWindow when end is not strictly after start.
// A site or window without orders is an empty Summary, not an error.
func (uc *GetExpectedDemand) Handle(ctx context.Context, location string, start, end time.Time) (demand.Summary, error) {
	if _, err := processcapacity.NewCapacityWindow(start, end); err != nil {
		return demand.Summary{}, err
	}
	return uc.Demand.Expected(ctx, location, start, end)
}
