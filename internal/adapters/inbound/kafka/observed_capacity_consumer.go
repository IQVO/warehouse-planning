package kafka

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	kafkago "github.com/segmentio/kafka-go"

	"github.com/claudioed/warehouse-planning/internal/adapters/kafka/cloudevents"
	"github.com/claudioed/warehouse-planning/internal/application/usecases"
	"github.com/claudioed/warehouse-planning/internal/domain/observed"
)

// FulfillmentTopic is fulfillment-execution's integration topic. This service
// knows nothing else about that context beyond this topic name and the
// CloudEvents type/payload shape below, read from fulfillment-execution's own
// apis/asyncapi.yaml on develop (docs/adr/0017).
const FulfillmentTopic = "warehouse.fulfillment.events"

// typeTaskCompleted is fulfillment-execution's confirmed `type` string for a
// station finishing a claimed task. It is the ONLY type this consumer acts on;
// every other type on the topic (TaskAvailable, TaskClaimed, TaskCPTMissed,
// PackageManifested, Transfer*) is ignored.
const typeTaskCompleted = "com.warehouse.wes.fulfillment-execution.task.TaskCompleted"

// observedCapacityConsumerName namespaces this consumer's rows in
// ports.ProcessedEventRepository.
const observedCapacityConsumerName = "observed-capacity-consumer"

// taskCompletedData mirrors (the subset of) fulfillment-execution's
// TaskCompleted `data` this service reads -- hand-mirrored, never imported from
// that service's Go types. The payload carries no site and no process path:
// task_type (the process STEP) is the only attribution (docs/adr/0017).
type taskCompletedData struct {
	TaskID string `json:"task_id"`
	// TaskType is omitted by the producer only when the task can no longer be
	// found; such a completion cannot be attributed to a step.
	TaskType string `json:"task_type"`
	// DurationSeconds is omitted when the claim time is unknown.
	DurationSeconds *int64 `json:"duration_seconds"`
}

// ObservedCapacityConsumer maintains the observed-capacity read model from
// fulfillment-execution's TaskCompleted events: completed tasks per (site,
// process step, minute). Every completion is attributed to ONE configured site,
// Location: the events carry none (docs/adr/0017 decision 3).
//
// Delivery is at-least-once with an atomic effect: the processed-event claim
// and the additive bucket write are ONE unit of work
// (usecases.RecordObservedCompletion), so a redelivered CloudEvents id counts
// once, and the offset is committed only after HandleMessage returned nil (or
// the message was dead-lettered).
type ObservedCapacityConsumer struct {
	Reader Reader
	Record *usecases.RecordObservedCompletion
	// Location is the site every consumed completion is attributed to
	// (DEMAND_SITE_ID).
	Location string
	Logger   *slog.Logger
	Retry    RetryPolicy

	// DLQ is this consumer's dead-letter writer (topic FulfillmentTopic +
	// DLQSuffix). A deterministic failure is published there at once; a
	// transient failure is retried domainMaxHandlerAttempts times first.
	DLQ      DeadLetterWriter
	DLQTopic string

	sleep sleepFunc // test hook; nil => real, ctx-cancellable sleep
}

// NewObservedCapacityConsumer constructs an ObservedCapacityConsumer reading
// FulfillmentTopic from brokers under groupID (an env-configured value supplied
// by the composition root -- never a literal, see
// TestKafkaConsumerGroupNeverHardcodedInline). A group without a committed
// offset starts from the earliest retained message, so a fresh deployment
// rebuilds the model from history.
func NewObservedCapacityConsumer(
	brokers []string,
	groupID, location string,
	record *usecases.RecordObservedCompletion,
	logger *slog.Logger,
) *ObservedCapacityConsumer {
	return &ObservedCapacityConsumer{
		Reader:   kafkago.NewReader(readerConfig(brokers, FulfillmentTopic, groupID)),
		Record:   record,
		Location: location,
		Logger:   defaultLogger(logger),
		DLQ:      newDomainDLQWriter(brokers),
		DLQTopic: FulfillmentTopic + DLQSuffix,
	}
}

// Run consumes FulfillmentTopic until ctx is cancelled or the reader fails,
// with the same at-least-once loop as the other consumers.
func (c *ObservedCapacityConsumer) Run(ctx context.Context) error {
	loop := consumeLoop{
		reader: c.Reader,
		handle: func(ctx context.Context, msg kafkago.Message) error { return c.HandleMessage(ctx, msg.Value) },
		logger: defaultLogger(c.Logger),
		name:   "observed capacity consumer",
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
// TaskCompleted with a usable payload, counts it in the read model.
//
// It returns nil for an unrecognized `type` (the topic carries many task and
// package events) and for a duplicate event id. It returns an error wrapping
// errPoison for a DETERMINISTIC failure -- not CloudEvents 1.0, a malformed
// payload, a blank task id, a subject that is not the task id, a missing
// task_type (the completion cannot be attributed to a step), a negative
// duration, no event time -- which the run loop dead-letters without retrying.
// Any other error is a transient infrastructure failure, after the unit of work
// rolled back, so the same message is retried.
func (c *ObservedCapacityConsumer) HandleMessage(ctx context.Context, value []byte) error {
	e, err := cloudevents.Decode(value)
	if err != nil {
		return poisonError("not a CloudEvents 1.0 message", err)
	}
	if e.Type() != typeTaskCompleted {
		return nil
	}
	completion, err := c.completionFrom(e.Subject(), e.Time(), e.DataAs)
	if err != nil {
		return poisonError(fmt.Sprintf("%s %s", e.Type(), e.ID()), err)
	}

	outcome, err := c.Record.Handle(ctx, observedCapacityConsumerName, e.ID(), completion)
	if err != nil {
		return fmt.Errorf("record observed completion: %w", err)
	}
	if outcome == usecases.OutcomeDuplicate {
		c.Logger.InfoContext(ctx, "skipping already-processed task event", "event_id", e.ID())
	}
	return nil
}

var errBlankTaskID = errors.New("task_id is required and must equal the subject")

func (c *ObservedCapacityConsumer) completionFrom(subject string, at time.Time, dataAs func(any) error) (observed.Completion, error) {
	var data taskCompletedData
	if err := dataAs(&data); err != nil {
		return observed.Completion{}, fmt.Errorf("malformed payload: %w", err)
	}
	if data.TaskID == "" || data.TaskID != subject {
		return observed.Completion{}, errBlankTaskID
	}
	return observed.NewCompletion(observed.CompletionParams{
		Location:        c.Location,
		ProcessType:     data.TaskType,
		At:              at,
		DurationSeconds: data.DurationSeconds,
	})
}

// Close releases the underlying Kafka reader and dead-letter writer.
func (c *ObservedCapacityConsumer) Close() error {
	err := c.Reader.Close()
	if c.DLQ != nil {
		err = errors.Join(err, c.DLQ.Close())
	}
	return err
}
