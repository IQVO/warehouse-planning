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
// Publish(), supersedes the plan published before it under the same
// capacityplan.SupersedeKey (docs/adr/0024 decision 3), saves both and stores
// EVERY event they recorded in the outbox -- all in ONE ports.UnitOfWork.Do,
// so a plan is never PUBLISHED without its events being queued, nor the
// reverse. The events are queued in this order: CapacityPlanSuperseded (of
// the older plan, when there is one), CapacityPlanPublished, then
// CapacityShortageDetected + BottleneckDetected when there is a shortage.
// Publishing an already PUBLISHED plan returns
// capacityplan.ErrAlreadyPublished, a SUPERSEDED one
// capacityplan.ErrSuperseded; neither changes anything.
type PublishCapacityPlan struct {
	Plans      ports.CapacityPlanRepository
	Outbox     ports.OutboxRepository
	Encoder    ports.EventEncoder
	UnitOfWork ports.UnitOfWork

	// Now supplies the publish time (utcNow when nil).
	Now func() time.Time
}

// Handle publishes the plan identified by id and returns it. A
// ports.ErrVersionConflict (a concurrent publish of the same key won the one
// PUBLISHED slot between the load and the save) reruns the whole unit once,
// which then supersedes the winner; a second one is returned (REST: 409).
func (uc *PublishCapacityPlan) Handle(ctx context.Context, id string) (*capacityplan.CapacityPlan, error) {
	now := uc.Now
	if now == nil {
		now = utcNow
	}

	var plan *capacityplan.CapacityPlan
	err := retryOnVersionConflict(func() error {
		var err error
		plan, err = uc.publish(ctx, id, now())
		return err
	})
	if err != nil {
		return nil, err
	}
	return plan, nil
}

// publish is one attempt: load, publish, supersede, save, queue.
func (uc *PublishCapacityPlan) publish(ctx context.Context, id string, at time.Time) (*capacityplan.CapacityPlan, error) {
	var plan *capacityplan.CapacityPlan
	err := uc.UnitOfWork.Do(ctx, func(ctx context.Context) error {
		found, err := uc.Plans.FindByID(ctx, id)
		if err != nil {
			return err
		}
		if found == nil {
			return ErrCapacityPlanNotFound
		}
		if err := found.Publish(at); err != nil {
			return err
		}
		events, err := uc.supersedeOlder(ctx, found, at)
		if err != nil {
			return err
		}
		if err := uc.Plans.Save(ctx, found); err != nil {
			return err
		}
		if err := enqueue(ctx, uc.Encoder, uc.Outbox, append(events, found.PullEvents()...)); err != nil {
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

// supersedeOlder supersedes the PUBLISHED plan of newer's key, if there is
// one, and returns its events. The older plan is saved BEFORE newer: one
// PUBLISHED plan per key holds after every statement, not only at commit.
func (uc *PublishCapacityPlan) supersedeOlder(ctx context.Context, newer *capacityplan.CapacityPlan, at time.Time) ([]capacityplan.Event, error) {
	older, err := uc.Plans.FindPublished(ctx, newer.SupersedeKey(), newer.ID())
	if err != nil || older == nil {
		return nil, err
	}
	if err := older.Supersede(newer.ID(), at); err != nil {
		return nil, err
	}
	if err := uc.Plans.Save(ctx, older); err != nil {
		return nil, err
	}
	return older.PullEvents(), nil
}
