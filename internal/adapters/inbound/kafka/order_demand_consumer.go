package kafka

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	kafkago "github.com/segmentio/kafka-go"

	"github.com/claudioed/warehouse-planning/internal/adapters/kafka/cloudevents"
	"github.com/claudioed/warehouse-planning/internal/application/usecases"
	"github.com/claudioed/warehouse-planning/internal/domain/demand"
)

// OrderTopic is order-management's integration topic. This service knows
// nothing else about that context beyond this topic name and the CloudEvents
// type/payload shapes below, read from order-management's own
// apis/asyncapi.yaml (docs/adr/0004-demand-ingestion-from-order-management.md).
const OrderTopic = "warehouse.order-management.events"

// order-management's confirmed `type` strings (its apis/asyncapi.yaml,
// channel warehouse.order-management.events -- never guessed). They are the
// ONLY types this consumer acts on:
//
//   - OrderAllocated: every line allocated (and every eligible line released
//     in the same pass); carries the order's promise cutoff.
//   - OrderPartiallyAllocated: some lines allocated and released, some
//     backordered, on a partial-shipment order; same payload shape.
//
// OrderRepromised is deliberately NOT consumed: it carries cpt ids and a
// reason but no new cutoff instant, so it cannot move an order between
// windows. Every other type is ignored.
const (
	typeOrderAllocated          = "com.warehouse.wes.order-management.order.OrderAllocated"
	typeOrderPartiallyAllocated = "com.warehouse.wes.order-management.order.OrderPartiallyAllocated"
)

// orderDemandConsumerName namespaces this consumer's rows in
// ports.ProcessedEventRepository.
const orderDemandConsumerName = "order-demand-consumer"

// orderAllocationData mirrors (the subset of) order-management's
// OrderAllocated / OrderPartiallyAllocated `data` this service reads --
// hand-mirrored, never imported from that service's Go types. promise_date
// is the order's promise cutoff instant; lines is exactly the set of lines
// released in this allocation pass (it may be empty).
type orderAllocationData struct {
	OrderID     string      `json:"order_id"`
	PromiseDate time.Time   `json:"promise_date"`
	Lines       []orderLine `json:"lines"`
}

type orderLine struct {
	LineNo int `json:"line_no"`
}

// OrderDemandConsumer maintains the expected-demand read model from
// order-management's OrderAllocated / OrderPartiallyAllocated events. Every
// order is attributed to ONE configured site, Location: order-management's
// events carry no fulfillment site (ADR 0004).
//
// Delivery is at-least-once with an atomic effect: the processed-event claim
// and the read-model upsert are ONE unit of work (usecases.RecordOrderDemand)
// and the offset is committed only after HandleMessage returned nil.
type OrderDemandConsumer struct {
	Reader Reader
	Record *usecases.RecordOrderDemand
	// Location is the site every consumed order is attributed to
	// (DEMAND_SITE_ID).
	Location string
	Logger   *slog.Logger
	Retry    RetryPolicy

	sleep sleepFunc // test hook; nil => real, ctx-cancellable sleep
}

// NewOrderDemandConsumer constructs an OrderDemandConsumer reading OrderTopic
// from brokers under groupID (an env-configured value supplied by the
// composition root -- never a literal, see
// TestKafkaConsumerGroupNeverHardcodedInline). A group without a committed
// offset starts from the earliest retained message, so a fresh deployment
// rebuilds the model from history.
func NewOrderDemandConsumer(
	brokers []string,
	groupID, location string,
	record *usecases.RecordOrderDemand,
	logger *slog.Logger,
) *OrderDemandConsumer {
	return &OrderDemandConsumer{
		Reader:   kafkago.NewReader(readerConfig(brokers, OrderTopic, groupID)),
		Record:   record,
		Location: location,
		Logger:   defaultLogger(logger),
	}
}

// Run consumes OrderTopic until ctx is cancelled or the reader fails, with
// the same at-least-once loop as the other consumers: commit only after
// success, retry the SAME message with capped backoff on a transient error.
func (c *OrderDemandConsumer) Run(ctx context.Context) error {
	loop := consumeLoop{
		reader: c.Reader,
		handle: func(ctx context.Context, msg kafkago.Message) error { return c.HandleMessage(ctx, msg.Value) },
		logger: defaultLogger(c.Logger),
		name:   "order demand consumer",
		retry:  c.Retry,
		sleep:  c.sleep,
	}
	return loop.run(ctx)
}

// HandleMessage decodes one CloudEvents 1.0 message and, if it is an
// OrderAllocated or OrderPartiallyAllocated with a usable payload, writes the
// order into the read model.
//
// It returns nil for anything deterministic -- failed CloudEvents validation
// (including the retired flat envelope), an unrecognized `type`, a malformed
// payload, a missing order id / promise date / event time, a subject that
// does not match the order id, a duplicate event id -- because retrying can
// never succeed. It returns a non-nil error ONLY for transient
// infrastructure failures, after the unit of work rolled back (nothing
// written, claim not recorded), so the same message is retried.
func (c *OrderDemandConsumer) HandleMessage(ctx context.Context, value []byte) error {
	e, err := cloudevents.Decode(value)
	if err != nil {
		c.Logger.WarnContext(ctx, "skipping non-CloudEvents order message", "error", err)
		return nil
	}
	if e.Type() != typeOrderAllocated && e.Type() != typeOrderPartiallyAllocated {
		return nil
	}

	var data orderAllocationData
	if err := e.DataAs(&data); err != nil {
		c.Logger.WarnContext(ctx, "skipping malformed order allocation payload", "error", err, "event_id", e.ID(), "type", e.Type())
		return nil
	}
	if data.OrderID != e.Subject() {
		c.Logger.WarnContext(ctx, "skipping order allocation whose subject is not its order id", "event_id", e.ID(), "subject", e.Subject(), "order_id", data.OrderID)
		return nil
	}
	order, err := demand.NewOrder(demand.OrderParams{
		OrderID:       data.OrderID,
		Location:      c.Location,
		PromiseAt:     data.PromiseDate,
		ReleasedLines: len(data.Lines),
		AsOf:          e.Time(),
	})
	if err != nil {
		c.Logger.WarnContext(ctx, "skipping order allocation that failed validation", "error", err, "event_id", e.ID(), "type", e.Type())
		return nil
	}

	outcome, err := c.Record.Handle(ctx, orderDemandConsumerName, e.ID(), order)
	if err != nil {
		return fmt.Errorf("record order demand: %w", err)
	}
	switch outcome {
	case usecases.OutcomeDuplicate:
		c.Logger.InfoContext(ctx, "skipping already-processed order event", "event_id", e.ID())
	case usecases.OutcomeStale:
		c.Logger.InfoContext(ctx, "order event is older than the stored order; model unchanged", "event_id", e.ID(), "order_id", data.OrderID)
	}
	return nil
}

// Close releases the underlying Kafka reader.
func (c *OrderDemandConsumer) Close() error {
	return c.Reader.Close()
}
