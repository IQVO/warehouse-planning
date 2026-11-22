package kafka

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	kafkago "github.com/segmentio/kafka-go"

	"github.com/claudioed/warehouse-planning/internal/adapters/kafka/cloudevents"
	"github.com/claudioed/warehouse-planning/internal/application/ports"
	"github.com/claudioed/warehouse-planning/internal/application/usecases"
	"github.com/claudioed/warehouse-planning/internal/domain/processflow"
)

// ProcessPathManagementTopic is process-path-management's INTEGRATION topic.
// This service knows nothing else about that context beyond this topic name
// and the CloudEvents type/payload shape below, pinned by docs/adr/0025
// decision 1 from process-path-management's ADR 0042 and documented in
// apis/asyncapi.yaml. The producer's analytics topic is never read: it belongs
// to its own projector.
const ProcessPathManagementTopic = "warehouse.process-path-management.events"

// process-path-management's `type` strings for the flow events. They are the
// ONLY types this consumer acts on; the full string is matched, never a
// suffix. Every other type on the topic (ProcessPathCreated, ProcessPathUpdated,
// ProcessPathDeactivated, CPTScheduleChanged) is acknowledged and ignored.
const (
	typeProcessFlowDefined = "com.warehouse.wes.process-path-management.processflow.ProcessFlowDefined"
	typeProcessFlowRevised = "com.warehouse.wes.process-path-management.processflow.ProcessFlowRevised"
	typeProcessFlowRetired = "com.warehouse.wes.process-path-management.processflow.ProcessFlowRetired"
)

// processFlowConsumerName namespaces this consumer's rows in
// ports.ProcessedEventRepository.
const processFlowConsumerName = "process-flow-consumer"

// processFlowData mirrors (the subset of) process-path-management's
// ProcessFlow* `data` this service reads -- hand-mirrored, never imported from
// that service's Go types. The three types carry the same full-state shape.
type processFlowData struct {
	FlowID string `json:"flow_id"`
	Name   string `json:"name"`
	Stages []struct {
		PathID      string `json:"path_id"`
		ProcessType string `json:"process_type"`
	} `json:"stages"`
	Status  string `json:"status"`
	Version int64  `json:"version"`
}

// ProcessFlowConsumer maintains the local copy of process-path-management's
// process flows (docs/adr/0025): one record per flow id, replaced only by a
// greater version, resolved at read time when a path id is looked up.
//
// Delivery is at-least-once with an atomic effect: the processed-event claim
// and the flow write are ONE unit of work (usecases.RecordProcessFlow) and the
// offset is committed only after HandleMessage returned nil (or the message was
// dead-lettered). A consumer that applies a sibling's events raises no event of
// its own (no loop). cmd/mcp and cmd/a2a read the copy and never start this
// consumer.
type ProcessFlowConsumer struct {
	Reader Reader
	Record *usecases.RecordProcessFlow
	Logger *slog.Logger
	Retry  RetryPolicy

	// Metrics counts the messages by outcome (applied, stale, duplicate,
	// rejected). Nil is a no-op.
	Metrics ports.ProcessFlowMetrics

	// DLQ is this consumer's dead-letter writer (topic
	// ProcessPathManagementTopic + DLQSuffix). A deterministic failure is
	// published there at once; a transient failure is retried
	// domainMaxHandlerAttempts times first.
	DLQ      DeadLetterWriter
	DLQTopic string

	sleep sleepFunc // test hook; nil => real, ctx-cancellable sleep
}

// NewProcessFlowConsumer constructs a ProcessFlowConsumer reading
// ProcessPathManagementTopic from brokers under groupID (an env-configured
// value supplied by the composition root -- never a literal, see
// TestKafkaConsumerGroupNeverHardcodedInline). A group without a committed
// offset starts from the earliest retained message, so a fresh deployment
// rebuilds the copy from history (and the producer republishes every ACTIVE
// flow at its start).
func NewProcessFlowConsumer(
	brokers []string,
	groupID string,
	record *usecases.RecordProcessFlow,
	metrics ports.ProcessFlowMetrics,
	logger *slog.Logger,
) *ProcessFlowConsumer {
	return &ProcessFlowConsumer{
		Reader:   kafkago.NewReader(readerConfig(brokers, ProcessPathManagementTopic, groupID)),
		Record:   record,
		Logger:   defaultLogger(logger),
		Metrics:  metrics,
		DLQ:      newDomainDLQWriter(brokers),
		DLQTopic: ProcessPathManagementTopic + DLQSuffix,
	}
}

// Run consumes ProcessPathManagementTopic until ctx is cancelled or the reader
// fails, with the same at-least-once loop as the other consumers: commit only
// after success, retry the SAME message with capped backoff on a transient
// error up to domainMaxHandlerAttempts times, and dead-letter a deterministic
// failure at once.
func (c *ProcessFlowConsumer) Run(ctx context.Context) error {
	loop := consumeLoop{
		reader: c.Reader,
		handle: func(ctx context.Context, msg kafkago.Message) error { return c.HandleMessage(ctx, msg.Value) },
		logger: defaultLogger(c.Logger),
		name:   "process flow consumer",
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
// ProcessFlowDefined, ProcessFlowRevised or ProcessFlowRetired with a usable
// payload, applies it to the local copy when its version is greater than the
// stored one.
//
// It returns nil for an unrecognized `type` (ProcessPath* and CPTScheduleChanged
// share the topic), for a duplicate event id and for an equal or lower version.
// It returns an error wrapping errPoison for a DETERMINISTIC failure -- not
// CloudEvents 1.0, a malformed payload, a missing flow_id, no stages, a stage
// with no path_id or a process_type outside the closed set, a status other than
// ACTIVE or RETIRED, a version below 1, no event time -- which the run loop
// dead-letters without retrying (the message is never applied). Any other error
// is a transient infrastructure failure, after the unit of work rolled back
// (nothing written, claim not recorded), so the same message is retried.
func (c *ProcessFlowConsumer) HandleMessage(ctx context.Context, value []byte) error {
	e, err := cloudevents.Decode(value)
	if err != nil {
		c.count(ctx, ports.ProcessFlowRejected)
		return poisonError("not a CloudEvents 1.0 message", err)
	}
	switch e.Type() {
	case typeProcessFlowDefined, typeProcessFlowRevised, typeProcessFlowRetired:
	default:
		return nil
	}
	flow, err := flowFrom(e.Time(), e.DataAs)
	if err != nil {
		c.count(ctx, ports.ProcessFlowRejected)
		c.Logger.WarnContext(ctx, "rejected process-path-management flow event", "event_id", sanitizeLog(e.ID()), "type", sanitizeLog(e.Type()), "error", sanitizeErr(err))
		return poisonError(fmt.Sprintf("%s %s", e.Type(), e.ID()), err)
	}

	outcome, err := c.Record.Handle(ctx, processFlowConsumerName, e.ID(), flow)
	if err != nil {
		return fmt.Errorf("record process flow: %w", err)
	}
	c.count(ctx, string(outcome))
	switch outcome {
	case usecases.OutcomeDuplicate:
		c.Logger.InfoContext(ctx, "skipping already-processed process-path-management event", "event_id", sanitizeLog(e.ID()))
	case usecases.OutcomeStale:
		c.Logger.InfoContext(ctx, "process flow version already in the copy; unchanged", "event_id", sanitizeLog(e.ID()), "flow_id", sanitizeLog(flow.ID()), "version", flow.Version())
	case usecases.OutcomeApplied:
		c.Logger.InfoContext(ctx, "process flow applied", "event_id", sanitizeLog(e.ID()), "flow_id", sanitizeLog(flow.ID()), "version", flow.Version(), "status", sanitizeLog(string(flow.Status())))
	}
	return nil
}

// count records one message outcome (a nil Metrics is a no-op).
func (c *ProcessFlowConsumer) count(ctx context.Context, outcome string) {
	if c.Metrics != nil {
		c.Metrics.ProcessFlowMessage(ctx, outcome)
	}
}

func flowFrom(at time.Time, dataAs func(any) error) (processflow.Flow, error) {
	var data processFlowData
	if err := dataAs(&data); err != nil {
		return processflow.Flow{}, fmt.Errorf("malformed payload: %w", err)
	}
	stages := make([]processflow.Stage, 0, len(data.Stages))
	for _, s := range data.Stages {
		stages = append(stages, processflow.Stage{PathID: s.PathID, ProcessType: processflow.ProcessType(s.ProcessType)})
	}
	return processflow.New(processflow.Params{
		FlowID:    data.FlowID,
		Name:      data.Name,
		Stages:    stages,
		Status:    processflow.Status(data.Status),
		Version:   data.Version,
		UpdatedAt: at,
	})
}

// Close releases the underlying Kafka reader and dead-letter writer.
func (c *ProcessFlowConsumer) Close() error {
	err := c.Reader.Close()
	if c.DLQ != nil {
		err = errors.Join(err, c.DLQ.Close())
	}
	return err
}
