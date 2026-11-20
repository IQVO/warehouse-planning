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
	"github.com/claudioed/warehouse-planning/internal/domain/laborstandard"
)

// LaborPerformanceTopic is labor-performance's INTEGRATION topic. This service
// knows nothing else about that context beyond this topic name and the
// CloudEvents type/payload shape below, pinned by docs/adr/0022 decision 2
// from labor-performance's ADR 0038 (both standard events join the topic with
// the analytics payload unchanged) and documented in apis/asyncapi.yaml. The
// producer's analytics topic is never read: it belongs to its own projector.
const LaborPerformanceTopic = "warehouse.labor-performance.events"

// labor-performance's `type` strings for the standard events. They are the
// ONLY types this consumer acts on; the full string is matched, never a
// suffix. Every other type on the topic (TaskPerformanceRecorded, ...) is
// ignored.
const (
	typeLaborStandardDefined = "com.warehouse.wes.labor-performance.standard.LaborStandardDefined"
	typeLaborStandardRevised = "com.warehouse.wes.labor-performance.standard.LaborStandardRevised"
)

// laborStandardConsumerName namespaces this consumer's rows in
// ports.ProcessedEventRepository.
const laborStandardConsumerName = "labor-standard-consumer"

// laborStandardData mirrors (the subset of) labor-performance's
// LaborStandardDefined / LaborStandardRevised `data` this service reads --
// hand-mirrored, never imported from that service's Go types. Both events
// carry the same shape; previous_expected_seconds (Revised only) and the
// team-lift fields are ignored. site_code is labor-performance ADR 0038's
// optional site scope: absent on every message produced before it.
type laborStandardData struct {
	StandardID             string    `json:"standard_id"`
	TaskType               string    `json:"task_type"`
	ExpectedSeconds        int64     `json:"expected_seconds"`
	TravelComponentSeconds *int64    `json:"travel_component_seconds"`
	HandlingUnit           string    `json:"handling_unit"`
	SiteCode               string    `json:"site_code"`
	EffectiveFrom          time.Time `json:"effective_from"`
}

// LaborStandardConsumer maintains the local copy of labor-performance's
// engineered labour standards (docs/adr/0022): one record per standard id,
// resolved at read time when a capacity plan prices its steps.
//
// Delivery is at-least-once with an atomic effect: the processed-event claim
// and the record write are ONE unit of work (usecases.RecordLaborStandard) and
// the offset is committed only after HandleMessage returned nil (or the
// message was dead-lettered). A consumer that applies a sibling's events
// raises no event of its own (no loop). cmd/mcp and cmd/a2a read the copy and
// never start this consumer.
type LaborStandardConsumer struct {
	Reader Reader
	Record *usecases.RecordLaborStandard
	Logger *slog.Logger
	Retry  RetryPolicy

	// DLQ is this consumer's dead-letter writer (topic LaborPerformanceTopic +
	// DLQSuffix). A deterministic failure is published there at once; a
	// transient failure is retried domainMaxHandlerAttempts times first.
	DLQ      DeadLetterWriter
	DLQTopic string

	sleep sleepFunc // test hook; nil => real, ctx-cancellable sleep
}

// NewLaborStandardConsumer constructs a LaborStandardConsumer reading
// LaborPerformanceTopic from brokers under groupID (an env-configured value
// supplied by the composition root -- never a literal, see
// TestKafkaConsumerGroupNeverHardcodedInline). A group without a committed
// offset starts from the earliest retained message, so a fresh deployment
// rebuilds the copy from history.
func NewLaborStandardConsumer(
	brokers []string,
	groupID string,
	record *usecases.RecordLaborStandard,
	logger *slog.Logger,
) *LaborStandardConsumer {
	return &LaborStandardConsumer{
		Reader:   kafkago.NewReader(readerConfig(brokers, LaborPerformanceTopic, groupID)),
		Record:   record,
		Logger:   defaultLogger(logger),
		DLQ:      newDomainDLQWriter(brokers),
		DLQTopic: LaborPerformanceTopic + DLQSuffix,
	}
}

// Run consumes LaborPerformanceTopic until ctx is cancelled or the reader
// fails, with the same at-least-once loop as the other consumers: commit only
// after success, retry the SAME message with capped backoff on a transient
// error up to domainMaxHandlerAttempts times, and dead-letter a deterministic
// failure at once.
func (c *LaborStandardConsumer) Run(ctx context.Context) error {
	loop := consumeLoop{
		reader: c.Reader,
		handle: func(ctx context.Context, msg kafkago.Message) error { return c.HandleMessage(ctx, msg.Value) },
		logger: defaultLogger(c.Logger),
		name:   "labor standard consumer",
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
// LaborStandardDefined or LaborStandardRevised with a usable payload, records
// it in the local copy.
//
// It returns nil for an unrecognized `type` (TaskPerformanceRecorded shares
// the topic) and for a duplicate event id or standard id. It returns an error
// wrapping errPoison for a DETERMINISTIC failure -- not CloudEvents 1.0, a
// malformed payload, a missing standard_id, task_type or effective_from, an
// expected_seconds that is missing or not above zero, a negative travel
// component, an unknown handling unit, no event time -- which the run loop
// dead-letters without retrying. Any other error is a transient
// infrastructure failure, after the unit of work rolled back (nothing written,
// claim not recorded), so the same message is retried.
func (c *LaborStandardConsumer) HandleMessage(ctx context.Context, value []byte) error {
	e, err := cloudevents.Decode(value)
	if err != nil {
		return poisonError("not a CloudEvents 1.0 message", err)
	}
	if e.Type() != typeLaborStandardDefined && e.Type() != typeLaborStandardRevised {
		return nil
	}
	standard, err := standardFrom(e.Time(), e.DataAs)
	if err != nil {
		return poisonError(fmt.Sprintf("%s %s", e.Type(), e.ID()), err)
	}

	outcome, err := c.Record.Handle(ctx, laborStandardConsumerName, e.ID(), standard)
	if err != nil {
		return fmt.Errorf("record labor standard: %w", err)
	}
	switch outcome {
	case usecases.OutcomeDuplicate:
		c.Logger.InfoContext(ctx, "skipping already-processed labor-performance event", "event_id", e.ID())
	case usecases.OutcomeStale:
		c.Logger.InfoContext(ctx, "labor standard id already in the copy; unchanged", "event_id", e.ID(), "standard_id", standard.ID())
	}
	return nil
}

func standardFrom(at time.Time, dataAs func(any) error) (laborstandard.Standard, error) {
	var data laborStandardData
	if err := dataAs(&data); err != nil {
		return laborstandard.Standard{}, fmt.Errorf("malformed payload: %w", err)
	}
	return laborstandard.New(laborstandard.Params{
		StandardID:             data.StandardID,
		TaskType:               data.TaskType,
		HandlingUnit:           laborstandard.HandlingUnit(data.HandlingUnit),
		SiteCode:               data.SiteCode,
		ExpectedSeconds:        data.ExpectedSeconds,
		TravelComponentSeconds: data.TravelComponentSeconds,
		EffectiveFrom:          data.EffectiveFrom,
		AsOf:                   at,
	})
}

// Close releases the underlying Kafka reader and dead-letter writer.
func (c *LaborStandardConsumer) Close() error {
	err := c.Reader.Close()
	if c.DLQ != nil {
		err = errors.Join(err, c.DLQ.Close())
	}
	return err
}
