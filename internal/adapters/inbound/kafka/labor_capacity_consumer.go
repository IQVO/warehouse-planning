package kafka

import (
	"context"
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
type LaborCapacityConsumer struct {
	Reader          Reader
	Register        *usecases.RegisterProcessCapacityConstraint
	ProcessedEvents ports.ProcessedEventRepository
	Logger          *slog.Logger
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
	logger *slog.Logger,
) *LaborCapacityConsumer {
	return &LaborCapacityConsumer{
		Reader:          kafkago.NewReader(readerConfig(brokers, LaborTopic, groupID)),
		Register:        register,
		ProcessedEvents: processedEvents,
		Logger:          defaultLogger(logger),
	}
}

// Run consumes LaborTopic until ctx is cancelled or the reader fails. A
// message HandleMessage cannot apply is logged and skipped -- it never
// stops the loop or blocks the partition.
func (c *LaborCapacityConsumer) Run(ctx context.Context) error {
	for {
		msg, err := c.Reader.ReadMessage(ctx)
		if err != nil {
			return err
		}
		if err := c.HandleMessage(ctx, msg.Value); err != nil {
			c.Logger.ErrorContext(ctx, "labor capacity event handling failed",
				"error", err, "partition", msg.Partition, "offset", msg.Offset)
		}
	}
}

// HandleMessage decodes one CloudEvents 1.0 message and, if it is a
// ShiftPlanCommitted, upserts the matching LABOR CapacityConstraint.
// Anything that fails CloudEvents validation, carries an unrecognized
// `type`, or has a malformed/empty-required-field payload is logged and
// skipped -- never an error that would stop the consumer.
func (c *LaborCapacityConsumer) HandleMessage(ctx context.Context, value []byte) error {
	e, err := cloudevents.Decode(value)
	if err != nil {
		c.Logger.WarnContext(ctx, "skipping non-CloudEvents labor message", "error", err)
		return nil
	}
	if e.Type() != typeShiftPlanCommitted {
		return nil
	}

	claimed, err := c.ProcessedEvents.Claim(ctx, laborConsumerName, e.ID())
	if err != nil {
		return err
	}
	if !claimed {
		c.Logger.InfoContext(ctx, "skipping already-processed ShiftPlanCommitted event", "event_id", e.ID())
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
	windowEnd := windowStart.Add(durationFromHours(data.PlannedHours))
	rate := float64(data.PlannedHeads) * data.PlannedRate

	_, err = c.Register.Handle(ctx, usecases.RegisterProcessCapacityConstraintCommand{
		ProcessType:    processcapacity.ProcessType(strings.ToUpper(data.PathID)),
		Location:       data.BuildingID,
		WindowStart:    windowStart,
		WindowEnd:      windowEnd,
		ConstraintType: processcapacity.ConstraintLabor,
		Quantity:       rate,
		Unit:           laborRateUnit,
		Period:         laborRatePeriod,
	})
	if err != nil {
		c.Logger.WarnContext(ctx, "skipping ShiftPlanCommitted that failed domain validation", "error", err, "event_id", e.ID())
		return nil
	}
	return nil
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
