package usecases

import (
	"context"
	"errors"
	"time"

	"github.com/claudioed/warehouse-planning/internal/application/ports"
	"github.com/claudioed/warehouse-planning/internal/domain/capacityplan"
)

// ErrCapacityPlanNotFound is returned when no CapacityPlan exists under
// the requested id.
var ErrCapacityPlanNotFound = errors.New("usecases: no CapacityPlan exists for this id")

// PublishCapacityPlan publishes a DRAFT plan: it loads it, calls
// Publish(), saves it and stores EVERY event the aggregate recorded
// (CapacityPlanPublished, and CapacityShortageDetected +
// BottleneckDetected when there is a shortage) in the outbox -- all in ONE
// ports.UnitOfWork.Do, so a plan is never PUBLISHED without its events
// being queued, nor the reverse. Publishing an already PUBLISHED plan
// returns capacityplan.ErrAlreadyPublished and changes nothing.
type PublishCapacityPlan struct {
	Plans      ports.CapacityPlanRepository
	Outbox     ports.OutboxRepository
	Encoder    ports.EventEncoder
	UnitOfWork ports.UnitOfWork

	// Now supplies the publish time (utcNow when nil).
	Now func() time.Time
}

// Handle publishes the plan identified by id and returns it.
func (uc *PublishCapacityPlan) Handle(ctx context.Context, id string) (*capacityplan.CapacityPlan, error) {
	now := uc.Now
	if now == nil {
		now = utcNow
	}

	var plan *capacityplan.CapacityPlan
	err := uc.UnitOfWork.Do(ctx, func(ctx context.Context) error {
		found, err := uc.Plans.FindByID(ctx, id)
		if err != nil {
			return err
		}
		if found == nil {
			return ErrCapacityPlanNotFound
		}
		if err := found.Publish(now()); err != nil {
			return err
		}
		if err := uc.Plans.Save(ctx, found); err != nil {
			return err
		}
		if err := enqueue(ctx, uc.Encoder, uc.Outbox, found.PullEvents()); err != nil {
			return err
		}
		plan = found
		return nil
	})
	if err != nil {
		return nil, err
	}
	return plan, nil
}
