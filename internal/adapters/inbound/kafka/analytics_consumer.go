package kafka

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	ce "github.com/cloudevents/sdk-go/v2/event"
	kafkago "github.com/segmentio/kafka-go"

	"github.com/claudioed/warehouse-planning/internal/adapters/kafka/cloudevents"
	"github.com/claudioed/warehouse-planning/internal/analytics/report"
)

// DLQSuffix is appended to the analytics topic to name its dead-letter
// topic (warehouse.warehouse-planning.analytics.dlq).
const DLQSuffix = ".dlq"

// analyticsEntity is the `entity` segment of every capacity-plan event type.
const analyticsEntity = "capacityplan"

// skipWarnEvery bounds the WARN lines for skipped (non-CloudEvents) messages:
// the first skip is logged, then at most one line per interval carrying the
// number suppressed since. A previous consumer of this service logged 100k
// lines replaying a topic of legacy messages.
const skipWarnEvery = time.Minute

// The four analytics event types this projector folds into the model. Every
// other CloudEvents type on the topic is acknowledged and ignored.
var analyticsKinds = map[string]report.Kind{
	cloudevents.Type(analyticsEntity, string(report.KindCreated)):            report.KindCreated,
	cloudevents.Type(analyticsEntity, string(report.KindPublished)):          report.KindPublished,
	cloudevents.Type(analyticsEntity, string(report.KindShortageDetected)):   report.KindShortageDetected,
	cloudevents.Type(analyticsEntity, string(report.KindBottleneckDetected)): report.KindBottleneckDetected,
}

// planData is the union of the analytics payload fields the projection reads
// (hand-mirrored from apis/asyncapi.yaml, never imported from the domain). A
// pointer field distinguishes "absent" from zero.
type planData struct {
	PlanID            string   `json:"plan_id"`
	WarehouseID       string   `json:"warehouse_id"`
	Location          string   `json:"location"`
	BottleneckStep    *string  `json:"bottleneck_step"`
	Shortage          *float64 `json:"shortage"`
	BindingConstraint *string  `json:"binding_constraint"`
}

// DeadLetterWriter is the subset of *kafkago.Writer the consumer needs.
type DeadLetterWriter interface {
	WriteMessages(ctx context.Context, msgs ...kafkago.Message) error
	Close() error
}

// AnalyticsConsumer is the projector's consumer: it reads the analytics topic
// under a FIXED, env-supplied consumer group (at-least-once, offsets
// committed only after success -- never a full-replay cache) and folds each
// capacity-plan event into the analytical model through report.Projection.
//
// Outcomes per message:
//   - not a CloudEvents 1.0 message (the retired flat envelope, garbage):
//     skipped and committed past, with a rate-limited WARN;
//   - a valid CloudEvent of any other type: ignored, committed past;
//   - a known type with an unusable payload, or one the store deterministically
//     rejects: dead-lettered at once (no pointless retries), committed past;
//   - a transient failure (database down, timeout): the SAME message is
//     retried with capped exponential backoff and the offset is NOT committed
//     -- it is never dead-lettered, because a database outage must not turn
//     into silently missing analytics.
type AnalyticsConsumer struct {
	Reader     Reader
	Projection report.Projection
	DLQ        DeadLetterWriter
	// DLQTopic is recorded in the x-dlq headers and the poison log line.
	DLQTopic string
	Logger   *slog.Logger
	Retry    RetryPolicy

	// Now is the clock of the skip-warning sampler (time.Now when nil).
	Now func() time.Time

	skips *skipSampler
	sleep sleepFunc // test hook; nil => real, ctx-cancellable sleep
}

// NewAnalyticsConsumer constructs the consumer for topic under groupID (an
// env-configured value from the composition root, never a literal). Its
// dead-letter writer targets topic + DLQSuffix. Nothing dials Kafka here.
func NewAnalyticsConsumer(brokers []string, topic, groupID string, projection report.Projection, logger *slog.Logger) *AnalyticsConsumer {
	return &AnalyticsConsumer{
		Reader:     kafkago.NewReader(readerConfig(brokers, topic, groupID)),
		Projection: projection,
		DLQ:        newDLQWriter(brokers, topic+DLQSuffix),
		DLQTopic:   topic + DLQSuffix,
		Logger:     defaultLogger(logger),
	}
}

// Run consumes until ctx is cancelled or the reader fails.
func (c *AnalyticsConsumer) Run(ctx context.Context) error {
	loop := consumeLoop{
		reader: c.Reader,
		handle: c.handle,
		logger: defaultLogger(c.Logger),
		name:   "analytics consumer",
		retry:  c.Retry,
		sleep:  c.sleep,
	}
	return loop.run(ctx)
}

// Close releases the reader and the dead-letter writer.
func (c *AnalyticsConsumer) Close() error {
	err := c.Reader.Close()
	if c.DLQ != nil {
		err = errors.Join(err, c.DLQ.Close())
	}
	return err
}

// HandleMessage processes one message; see the type comment for outcomes. A
// non-nil error is always transient.
func (c *AnalyticsConsumer) HandleMessage(ctx context.Context, msg kafkago.Message) error {
	return c.handle(ctx, msg)
}

func (c *AnalyticsConsumer) handle(ctx context.Context, msg kafkago.Message) error {
	evt, err := cloudevents.Decode(msg.Value)
	if err != nil {
		c.skipped(ctx, msg, err)
		return nil
	}
	kind, known := analyticsKinds[evt.Type()]
	if !known {
		defaultLogger(c.Logger).DebugContext(ctx, "analytics: ignoring event of another type", "type", evt.Type(), "event_id", evt.ID())
		return nil
	}
	planEvent, err := toPlanEvent(kind, evt)
	if err != nil {
		return c.deadLetter(ctx, msg, err)
	}
	if _, err := c.Projection.Apply(ctx, planEvent); err != nil {
		if errors.Is(err, report.ErrRejected) {
			return c.deadLetter(ctx, msg, err)
		}
		return fmt.Errorf("analytics: project %s %s: %w", kind, evt.ID(), err)
	}
	return nil
}

// toPlanEvent validates a decoded CloudEvent of a known kind and maps it to a
// PlanEvent. Every error is deterministic (the same bytes can never pass).
func toPlanEvent(kind report.Kind, evt ce.Event) (report.PlanEvent, error) {
	var d planData
	if err := evt.DataAs(&d); err != nil {
		return report.PlanEvent{}, fmt.Errorf("decode %s data: %w", kind, err)
	}
	switch {
	case d.PlanID == "" || d.PlanID != evt.Subject():
		return report.PlanEvent{}, fmt.Errorf("%s %s: plan_id %q does not match subject %q", kind, evt.ID(), d.PlanID, evt.Subject())
	case d.WarehouseID == "" || d.Location == "":
		return report.PlanEvent{}, fmt.Errorf("%s %s: warehouse_id and location are required", kind, evt.ID())
	case evt.Time().IsZero():
		return report.PlanEvent{}, fmt.Errorf("%s %s: time is required", kind, evt.ID())
	case d.Shortage != nil && *d.Shortage < 0:
		return report.PlanEvent{}, fmt.Errorf("%s %s: shortage %v is negative", kind, evt.ID(), *d.Shortage)
	}
	at := evt.Time().UTC()
	e := report.PlanEvent{
		Kind: kind, EventID: evt.ID(), At: at, PlanID: d.PlanID, WarehouseID: d.WarehouseID, Location: d.Location,
		BottleneckStep: d.BottleneckStep, Shortage: d.Shortage,
	}
	switch kind {
	case report.KindCreated:
		e.CreatedAt = &at
	case report.KindPublished:
		e.PublishedAt = &at
		e.BindingConstraint = d.BindingConstraint
	case report.KindShortageDetected, report.KindBottleneckDetected:
		// carry only the step/shortage already mapped above
	}
	return e, nil
}

// skipped logs a skipped non-CloudEvents message, sampled.
func (c *AnalyticsConsumer) skipped(ctx context.Context, msg kafkago.Message, cause error) {
	if c.skips == nil {
		now := c.Now
		if now == nil {
			now = time.Now
		}
		c.skips = &skipSampler{every: skipWarnEvery, now: now}
	}
	if emit, suppressed := c.skips.allow(); emit {
		defaultLogger(c.Logger).WarnContext(ctx, "analytics: skipping message that is not a CloudEvents 1.0 event (further skips are counted, not logged, until the next interval)",
			"error", cause, "topic", msg.Topic, "partition", msg.Partition, "offset", msg.Offset, "suppressed_since_last_warning", suppressed)
	}
}

// deadLetter publishes the raw, unmodified message to the DLQ with error
// context in headers, then lets the loop commit past it. A DLQ write failure
// is returned (transient: the loop retries the message) so a poison message
// is never lost.
func (c *AnalyticsConsumer) deadLetter(ctx context.Context, msg kafkago.Message, cause error) error {
	defaultLogger(c.Logger).ErrorContext(ctx, "analytics: dead-lettering a message that can never be projected",
		"error", cause, "dlq_topic", c.DLQTopic, "partition", msg.Partition, "offset", msg.Offset)
	headers := append([]kafkago.Header{}, msg.Headers...)
	headers = append(headers,
		kafkago.Header{Key: "x-dlq-source-topic", Value: []byte(msg.Topic)},
		kafkago.Header{Key: "x-dlq-error", Value: []byte(cause.Error())},
		kafkago.Header{Key: "x-dlq-failed-at", Value: []byte(time.Now().UTC().Format(time.RFC3339))},
	)
	if err := writeDLQ(ctx, c.DLQ, kafkago.Message{Key: msg.Key, Value: msg.Value, Headers: headers}); err != nil {
		return fmt.Errorf("analytics: publish to dead-letter topic %s: %w", c.DLQTopic, err)
	}
	return nil
}

// skipSampler lets the first call through, then at most one per interval,
// reporting how many were suppressed in between.
type skipSampler struct {
	mu         sync.Mutex
	every      time.Duration
	now        func() time.Time
	last       time.Time
	suppressed int64
}

func (s *skipSampler) allow() (emit bool, suppressed int64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	now := s.now()
	if s.last.IsZero() || now.Sub(s.last) >= s.every {
		suppressed, s.suppressed, s.last = s.suppressed, 0, now
		return true, suppressed
	}
	s.suppressed++
	return false, 0
}

// newDLQWriter builds the dead-letter writer. AllowAutoTopicCreation: the
// fleet leaves topic creation to the producing writer and the DLQ topic is
// only ever written on the rare poison path. A short BatchTimeout keeps a
// synchronous single-message write from waiting out kafka-go's 1s default.
func newDLQWriter(brokers []string, dlqTopic string) DeadLetterWriter {
	return &kafkago.Writer{
		Addr:                   kafkago.TCP(brokers...),
		Topic:                  dlqTopic,
		AllowAutoTopicCreation: true,
		BatchTimeout:           10 * time.Millisecond,
	}
}

const (
	dlqTopicReadyAttempts = 40
	dlqTopicReadyBackoff  = 250 * time.Millisecond
)

// writeDLQ publishes msg, retrying (bounded) while the auto-created DLQ topic
// has no leader yet. Any other error, or exhausting the budget, is returned.
func writeDLQ(ctx context.Context, w DeadLetterWriter, msg kafkago.Message) error {
	var err error
	for attempt := 0; attempt < dlqTopicReadyAttempts; attempt++ {
		if err = w.WriteMessages(ctx, msg); err == nil || !isTopicNotReady(err) {
			return err
		}
		select {
		case <-ctx.Done():
			return errors.Join(err, ctx.Err())
		case <-time.After(dlqTopicReadyBackoff):
		}
	}
	return err
}

func isTopicNotReady(err error) bool {
	var werrs kafkago.WriteErrors
	if errors.As(err, &werrs) {
		for _, e := range werrs {
			if e != nil && !isTopicNotReady(e) {
				return false
			}
		}
		return werrs.Count() > 0
	}
	return errors.Is(err, kafkago.UnknownTopicOrPartition) || errors.Is(err, kafkago.LeaderNotAvailable)
}
