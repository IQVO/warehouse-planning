package kafka

import (
	"context"
	"fmt"
	"log/slog"
	"strings"
	"time"

	kafkago "github.com/segmentio/kafka-go"

	"github.com/claudioed/warehouse-planning/internal/adapters/kafka/cloudevents"
	"github.com/claudioed/warehouse-planning/internal/application/ports"
	"github.com/claudioed/warehouse-planning/internal/application/usecases"
	"github.com/claudioed/warehouse-planning/internal/domain/processcapacity"
)

// LaborTopic is workforce-management's integration topic. This service has
// no business knowing anything else about that context beyond this topic
// name and the CloudEvents type/payload shape below (see
// .claude/skills/how-to-add-an-integration-event.md).
const LaborTopic = "warehouse.workforce.events"

// typeShiftPlanCommitted is workforce-management's confirmed `type`
// string (docs/adr/0001-...'s Addendum, read from its own
// apis/asyncapi.yaml on origin/develop -- never guessed).
const typeShiftPlanCommitted = "com.warehouse.wes.workforce-management.shiftplan.ShiftPlanCommitted"

// laborConsumerName namespaces this consumer's rows in
// ports.ProcessedEventRepository, so the same CloudEvents id processed by
// a different consumer (e.g. a future one) never collides with this one's
// idempotency claim.
const laborConsumerName = "labor-capacity-consumer"

// laborRateUnit is the native CapacityUnit this phase registers a LABOR
// constraint under. workforce-management's `planned_rate` field carries
// no explicit unit of its own in the confirmed contract (Addendum, Task
// 0.4) -- UNIT/HOUR is this phase's documented default, matching Phase
// 1's PICK/PACK worked examples, on the assumption that a path's planned
// rate is expressed in the same units/hour convention every other LABOR
// constraint in this domain already uses. Revisit if workforce-management
// later publishes an explicit unit.
const laborRateUnit = processcapacity.UnitUnit

// laborRatePeriod is the time base laborRateUnit is expressed over.
const laborRatePeriod = time.Hour

// shiftPlanCommittedData mirrors (a subset of) workforce-management's
// ShiftPlanCommitted payload -- hand-mirrored locally, never imported
// from that service's own Go types (see how-to-add-an-integration-event.md
// §"Never import the sibling's Go packages"). One Kafka message = one
// PathPlan line within a committed ShiftPlan (fan-out already performed
// by the producer).
type shiftPlanCommittedData struct {
	BuildingID   string  `json:"building_id"`
	ShiftID      string  `json:"shift_id"`
	PathID       string  `json:"path_id"`
	PlannedHeads int     `json:"planned_heads"`
	PlannedRate  float64 `json:"planned_rate"`
	PlannedHours float64 `json:"planned_hours"`
}

// LaborCapacityConsumer upserts a LABOR CapacityConstraint on the
// ProcessCapacity aggregate identified by (ProcessType derived from
// path_id, Location=building_id, window=[event time, event time +
// planned_hours)) for every ShiftPlanCommitted fan-out message it
// receives.
//
// UoW is REQUIRED: the processed-event claim and the constraint upsert run
// inside one UoW.Do, so they commit or roll back together. Retry tunes the
// run loop's backoff (zero value = defaults).
type LaborCapacityConsumer struct {
	Reader          Reader
	Register        *usecases.RegisterProcessCapacityConstraint
	ProcessedEvents ports.ProcessedEventRepository
	UoW             ports.UnitOfWork
	Logger          *slog.Logger
	Retry           RetryPolicy

	sleep sleepFunc // test hook; nil => real, ctx-cancellable sleep
}

// NewLaborCapacityConsumer constructs a LaborCapacityConsumer reading
// LaborTopic from brokers under groupID (an env-configured value supplied
// by the composition root -- never a literal, see
// TestKafkaConsumerGroupNeverHardcodedInline).
func NewLaborCapacityConsumer(
	brokers []string,
	groupID string,
	register *usecases.RegisterProcessCapacityConstraint,
	processedEvents ports.ProcessedEventRepository,
	uow ports.UnitOfWork,
	logger *slog.Logger,
) *LaborCapacityConsumer {
	return &LaborCapacityConsumer{
		Reader:          kafkago.NewReader(readerConfig(brokers, LaborTopic, groupID)),
		Register:        register,
		ProcessedEvents: processedEvents,
		UoW:             uow,
		Logger:          defaultLogger(logger),
	}
}

// Run consumes LaborTopic until ctx is cancelled or the reader fails. It is
// at-least-once: a message's offset is committed only after HandleMessage
// returned nil, and a transient failure retries the SAME message with
// capped exponential backoff (see consumeLoop) -- it is never skipped.
func (c *LaborCapacityConsumer) Run(ctx context.Context) error {
	loop := consumeLoop{
		reader: c.Reader,
		handle: func(ctx context.Context, msg kafkago.Message) error { return c.HandleMessage(ctx, msg.Value) },
		logger: defaultLogger(c.Logger),
		name:   "labor capacity consumer",
		retry:  c.Retry,
		sleep:  c.sleep,
	}
	return loop.run(ctx)
}

// HandleMessage decodes one CloudEvents 1.0 message and, if it is a
// ShiftPlanCommitted, upserts the matching LABOR CapacityConstraint.
//
// It returns nil for anything deterministic -- failed CloudEvents
// validation, an unrecognized `type`, a malformed/empty-required-field
// payload, a duplicate event id, or a domain-validation rejection (all
// logged) -- because retrying those can never succeed. It returns a
// non-nil error ONLY for transient/infrastructure failures (claim, find,
// save, begin/commit), after the unit of work has rolled back, so the
// caller may retry the same message safely.
func (c *LaborCapacityConsumer) HandleMessage(ctx context.Context, value []byte) error {
	e, err := cloudevents.Decode(value)
	if err != nil {
		c.Logger.WarnContext(ctx, "skipping non-CloudEvents labor message", "error", err)
		return nil
	}
	if e.Type() != typeShiftPlanCommitted {
		return nil
	}

	var data shiftPlanCommittedData
	if err := e.DataAs(&data); err != nil {
		c.Logger.WarnContext(ctx, "skipping malformed ShiftPlanCommitted payload", "error", err, "event_id", e.ID())
		return nil
	}
	if data.BuildingID == "" || data.PathID == "" {
		c.Logger.WarnContext(ctx, "skipping ShiftPlanCommitted with missing building_id/path_id", "event_id", e.ID())
		return nil
	}

	windowStart := e.Time()
	cmd := usecases.RegisterProcessCapacityConstraintCommand{
		ProcessType:    processcapacity.ProcessType(strings.ToUpper(data.PathID)),
		Location:       data.BuildingID,
		WindowStart:    windowStart,
		WindowEnd:      windowStart.Add(durationFromHours(data.PlannedHours)),
		ConstraintType: processcapacity.ConstraintLabor,
		Quantity:       float64(data.PlannedHeads) * data.PlannedRate,
		Unit:           laborRateUnit,
		Period:         laborRatePeriod,
	}

	// Claim + upsert are ONE transaction: if the upsert fails the claim
	// rolls back with it, so the retry is processed instead of skipped.
	return c.UoW.Do(ctx, func(ctx context.Context) error {
		claimed, err := c.ProcessedEvents.Claim(ctx, laborConsumerName, e.ID())
		if err != nil {
			return fmt.Errorf("claim processed event: %w", err)
		}
		if !claimed {
			c.Logger.InfoContext(ctx, "skipping already-processed ShiftPlanCommitted event", "event_id", e.ID())
			return nil
		}

		if _, err := c.Register.Handle(ctx, cmd); err != nil {
			if usecases.IsDomainValidationError(err) {
				c.Logger.WarnContext(ctx, "skipping ShiftPlanCommitted that failed domain validation", "error", err, "event_id", e.ID())
				return nil
			}
			return fmt.Errorf("register labor constraint: %w", err)
		}
		return nil
	})
}

// Close releases the underlying Kafka reader.
func (c *LaborCapacityConsumer) Close() error {
	return c.Reader.Close()
}

// durationFromHours converts a float64 hour count (workforce-management's
// planned_hours) into a time.Duration.
func durationFromHours(hours float64) time.Duration {
	return time.Duration(hours * float64(time.Hour))
}
