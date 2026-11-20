package kafka

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"time"

	kafkago "github.com/segmentio/kafka-go"

	"github.com/claudioed/warehouse-planning/internal/adapters/kafka/cloudevents"
	"github.com/claudioed/warehouse-planning/internal/application/ports"
	"github.com/claudioed/warehouse-planning/internal/application/usecases"
	"github.com/claudioed/warehouse-planning/internal/domain/labourplan"
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
// by the producer). site_code is the canonical site; building_id is its
// deprecated twin (same value), still read for messages produced before
// site_code existed (ADR 0021).
type shiftPlanCommittedData struct {
	SiteCode     string  `json:"site_code"`
	BuildingID   string  `json:"building_id"`
	ShiftID      string  `json:"shift_id"`
	PathID       string  `json:"path_id"`
	PlannedHeads int     `json:"planned_heads"`
	PlannedRate  float64 `json:"planned_rate"`
	PlannedHours float64 `json:"planned_hours"`
}

// LaborCapacityConsumer upserts a LABOR CapacityConstraint on the
// ProcessCapacity aggregate identified by (ProcessType derived from
// path_id, Location=site_code or else building_id, window=[event time, event time +
// planned_hours)) for every ShiftPlanCommitted fan-out message it
// receives. A line that carries a shift_id also feeds the labour plan drift
// (docs/adr/0023 decision 3) through Commitments.
//
// UoW is REQUIRED: the processed-event claim, the constraint upsert and the
// committed heads (with the drift they raise) run inside one UoW.Do, so they
// commit or roll back together. Retry tunes the run loop's backoff (zero
// value = defaults).
type LaborCapacityConsumer struct {
	Reader          Reader
	Register        *usecases.RegisterProcessCapacityConstraint
	ProcessedEvents ports.ProcessedEventRepository
	UoW             ports.UnitOfWork
	Logger          *slog.Logger
	Retry           RetryPolicy

	// Commitments is the commitment trigger of the labour plan drift, the
	// second step of the claim's unit of work (after the LABOR
	// registration, which it never changes). Nil records no commitment.
	Commitments *usecases.RecordShiftCommitment

	// DLQ is this consumer's dead-letter writer (topic LaborTopic +
	// DLQSuffix), built by NewLaborCapacityConsumer. A transient failure
	// is retried domainMaxHandlerAttempts times before the message is
	// published here instead of blocking the partition forever
	// (deadletter.go).
	DLQ      DeadLetterWriter
	DLQTopic string

	sleep sleepFunc // test hook; nil => real, ctx-cancellable sleep
}

// NewLaborCapacityConsumer constructs a LaborCapacityConsumer reading
// LaborTopic from brokers under groupID (an env-configured value supplied
// by the composition root -- never a literal, see
// TestKafkaConsumerGroupNeverHardcodedInline). commitments is the labour
// plan drift's commitment trigger (nil records no commitment).
func NewLaborCapacityConsumer(
	brokers []string,
	groupID string,
	register *usecases.RegisterProcessCapacityConstraint,
	commitments *usecases.RecordShiftCommitment,
	processedEvents ports.ProcessedEventRepository,
	uow ports.UnitOfWork,
	logger *slog.Logger,
) *LaborCapacityConsumer {
	return &LaborCapacityConsumer{
		Reader:          kafkago.NewReader(readerConfig(brokers, LaborTopic, groupID)),
		Register:        register,
		Commitments:     commitments,
		ProcessedEvents: processedEvents,
		UoW:             uow,
		Logger:          defaultLogger(logger),
		DLQ:             newDomainDLQWriter(brokers),
		DLQTopic:        LaborTopic + DLQSuffix,
	}
}

// Run consumes LaborTopic until ctx is cancelled or the reader fails. It is
// at-least-once: a message's offset is committed only after HandleMessage
// returned nil. A transient failure retries the SAME message with capped
// exponential backoff (see consumeLoop) up to domainMaxHandlerAttempts
// times, after which it is dead-lettered rather than blocking the
// partition forever.
func (c *LaborCapacityConsumer) Run(ctx context.Context) error {
	loop := consumeLoop{
		reader: c.Reader,
		handle: func(ctx context.Context, msg kafkago.Message) error { return c.HandleMessage(ctx, msg.Value) },
		logger: defaultLogger(c.Logger),
		name:   "labor capacity consumer",
		retry:  c.Retry,
		sleep:  c.sleep,
	}
	if c.DLQ != nil {
		loop.maxAttempts = domainMaxHandlerAttempts
		loop.deadLetter = func(ctx context.Context, msg kafkago.Message, cause error) error {
			return publishDeadLetter(ctx, c.DLQ, c.DLQTopic, msg, cause)
		}
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
	site := data.site()
	if site == "" || data.PathID == "" {
		c.Logger.WarnContext(ctx, "skipping ShiftPlanCommitted with missing site_code/building_id or path_id", "event_id", e.ID())
		return nil
	}

	windowStart := e.Time()
	cmd := usecases.RegisterProcessCapacityConstraintCommand{
		ProcessType:    processcapacity.ProcessType(strings.ToUpper(data.PathID)),
		Location:       site,
		WindowStart:    windowStart,
		WindowEnd:      windowStart.Add(durationFromHours(data.PlannedHours)),
		ConstraintType: processcapacity.ConstraintLabor,
		Quantity:       float64(data.PlannedHeads) * data.PlannedRate,
		Unit:           laborRateUnit,
		Period:         laborRatePeriod,
	}

	// Claim + upsert + committed heads are ONE transaction: if a later step
	// fails the claim rolls back with it, so the retry is processed instead
	// of skipped. A redelivered event (already claimed) does none of them.
	return c.UoW.Do(ctx, func(ctx context.Context) error {
		claimed, err := c.ProcessedEvents.Claim(ctx, laborConsumerName, e.ID())
		if err != nil {
			return fmt.Errorf("claim processed event: %w", err)
		}
		if !claimed {
			c.Logger.InfoContext(ctx, "skipping already-processed ShiftPlanCommitted event", "event_id", e.ID())
			return nil
		}

		if err := c.registerLabor(ctx, cmd, e.ID()); err != nil {
			return err
		}
		return c.recordCommitment(ctx, data, site, e.Time(), e.ID())
	})
}

// registerLabor upserts the LABOR constraint of the line. A domain-validation
// rejection is logged and skipped (nil); anything else is returned wrapped.
func (c *LaborCapacityConsumer) registerLabor(ctx context.Context, cmd usecases.RegisterProcessCapacityConstraintCommand, eventID string) error {
	if _, err := c.Register.Handle(ctx, cmd); err != nil {
		if usecases.IsDomainValidationError(err) {
			c.Logger.WarnContext(ctx, "skipping ShiftPlanCommitted that failed domain validation", "error", err, "event_id", eventID)
			return nil
		}
		return fmt.Errorf("register labor constraint: %w", err)
	}
	return nil
}

// recordCommitment is the commitment trigger of the labour plan drift
// (docs/adr/0023 decision 3), the second step of the claim's unit of work: it
// upserts the heads committed for (site, shift_id, path_id) and raises a
// LabourPlanDriftDetected when the drift against the stored labour plan is
// news. The path_id passes through labourplan.CanonicalPathID (trimmed, upper
// case), the form the labour plan is keyed on, so a line for "PICK" meets a
// plan PUT as "pick". It runs whatever the LABOR registration decided (the
// commitment is workforce-management's, not the constraint's). A line without
// shift_id, or no Commitments wired, records nothing; a rejected commitment
// (negative heads) is logged and skipped.
func (c *LaborCapacityConsumer) recordCommitment(ctx context.Context, data shiftPlanCommittedData, site string, observedAt time.Time, eventID string) error {
	shift := strings.TrimSpace(data.ShiftID)
	if c.Commitments == nil || shift == "" {
		return nil
	}
	err := c.Commitments.Handle(ctx, usecases.RecordShiftCommitmentCommand{
		SiteID: site, ShiftID: shift, PathID: labourplan.CanonicalPathID(data.PathID),
		CommittedHeads: data.PlannedHeads, ObservedAt: observedAt,
	})
	if usecases.IsCommitmentRejection(err) {
		c.Logger.WarnContext(ctx, "skipping the labour plan drift of a ShiftPlanCommitted with an invalid commitment", "error", err, "event_id", eventID)
		return nil
	}
	if err != nil {
		return fmt.Errorf("record committed heads: %w", err)
	}
	return nil
}

// Close releases the underlying Kafka reader and dead-letter writer.
func (c *LaborCapacityConsumer) Close() error {
	err := c.Reader.Close()
	if c.DLQ != nil {
		err = errors.Join(err, c.DLQ.Close())
	}
	return err
}

// site is the line's site: the canonical site_code, or the deprecated
// building_id when site_code is absent or blank (ADR 0021).
func (d shiftPlanCommittedData) site() string {
	if s := strings.TrimSpace(d.SiteCode); s != "" {
		return s
	}
	return strings.TrimSpace(d.BuildingID)
}

// durationFromHours converts a float64 hour count (workforce-management's
// planned_hours) into a time.Duration.
func durationFromHours(hours float64) time.Duration {
	return time.Duration(hours * float64(time.Hour))
}
