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
	"github.com/claudioed/warehouse-planning/internal/domain/inbound"
)

// InboundTopic is inbound-receiving's integration topic. This service knows
// nothing else about that context beyond this topic name and the CloudEvents
// type/payload shapes below, read from inbound-receiving's own
// apis/asyncapi.yaml on develop (docs/adr/0017).
const InboundTopic = "warehouse.inbound-receiving.events"

// inbound-receiving's confirmed `type` strings (its apis/asyncapi.yaml,
// channel warehouse.inbound-receiving.events -- never guessed). They are the
// ONLY types this consumer acts on:
//
//   - DockAppointmentBooked: a door window was booked for a carrier.
//   - DockAppointmentCancelled: a Booked appointment was cancelled (terminal).
//
// DockAppointmentCheckedIn / DockAppointmentCompleted (the appointment still
// happened, so it stays the demand it was) and every ASN or receipt event on
// the same topic are ignored.
const (
	typeDockAppointmentBooked    = "com.warehouse.wms.inbound-receiving.dockappointment.DockAppointmentBooked"
	typeDockAppointmentCancelled = "com.warehouse.wms.inbound-receiving.dockappointment.DockAppointmentCancelled"
)

// inboundDemandConsumerName namespaces this consumer's rows in
// ports.ProcessedEventRepository.
const inboundDemandConsumerName = "inbound-demand-consumer"

// dockAppointmentBookedData mirrors (the subset of) inbound-receiving's
// DockAppointmentBooked `data` this service reads -- hand-mirrored, never
// imported from that service's Go types. The payload carries no site, no
// units and no revision (docs/adr/0017).
type dockAppointmentBookedData struct {
	AppointmentID string    `json:"appointment_id"`
	WindowStart   time.Time `json:"window_start"`
	WindowEnd     time.Time `json:"window_end"`
	ASNNumbers    []string  `json:"asn_numbers"`
}

// dockAppointmentCancelledData mirrors DockAppointmentCancelled `data`.
type dockAppointmentCancelledData struct {
	AppointmentID string `json:"appointment_id"`
}

// InboundDemandConsumer maintains the inbound-demand read model from
// inbound-receiving's DockAppointmentBooked / DockAppointmentCancelled events.
// Every appointment is attributed to ONE configured site, Location: the events
// carry no site (docs/adr/0017 decision 3).
//
// Delivery is at-least-once with an atomic effect: the processed-event claim
// and the ordering-guarded write are ONE unit of work
// (usecases.RecordInboundAppointment) and the offset is committed only after
// HandleMessage returned nil (or the message was dead-lettered).
type InboundDemandConsumer struct {
	Reader Reader
	Record *usecases.RecordInboundAppointment
	// Location is the site every consumed appointment is attributed to
	// (DEMAND_SITE_ID).
	Location string
	Logger   *slog.Logger
	Retry    RetryPolicy

	// DLQ is this consumer's dead-letter writer (topic InboundTopic +
	// DLQSuffix). A deterministic failure is published there at once; a
	// transient failure is retried domainMaxHandlerAttempts times first.
	DLQ      DeadLetterWriter
	DLQTopic string

	sleep sleepFunc // test hook; nil => real, ctx-cancellable sleep
}

// NewInboundDemandConsumer constructs an InboundDemandConsumer reading
// InboundTopic from brokers under groupID (an env-configured value supplied by
// the composition root -- never a literal, see
// TestKafkaConsumerGroupNeverHardcodedInline). A group without a committed
// offset starts from the earliest retained message, so a fresh deployment
// rebuilds the model from history.
func NewInboundDemandConsumer(
	brokers []string,
	groupID, location string,
	record *usecases.RecordInboundAppointment,
	logger *slog.Logger,
) *InboundDemandConsumer {
	return &InboundDemandConsumer{
		Reader:   kafkago.NewReader(readerConfig(brokers, InboundTopic, groupID)),
		Record:   record,
		Location: location,
		Logger:   defaultLogger(logger),
		DLQ:      newDomainDLQWriter(brokers),
		DLQTopic: InboundTopic + DLQSuffix,
	}
}

// Run consumes InboundTopic until ctx is cancelled or the reader fails, with
// the same at-least-once loop as the other consumers: commit only after
// success, retry the SAME message with capped backoff on a transient error up
// to domainMaxHandlerAttempts times, and dead-letter a deterministic failure at
// once.
func (c *InboundDemandConsumer) Run(ctx context.Context) error {
	loop := consumeLoop{
		reader: c.Reader,
		handle: func(ctx context.Context, msg kafkago.Message) error { return c.HandleMessage(ctx, msg.Value) },
		logger: defaultLogger(c.Logger),
		name:   "inbound demand consumer",
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
// DockAppointmentBooked or DockAppointmentCancelled with a usable payload,
// applies it to the read model.
//
// It returns nil for an unrecognized `type` (the topic is shared with ASN and
// receipt events) and for a duplicate event id. It returns an error wrapping
// errPoison for a DETERMINISTIC failure -- not CloudEvents 1.0 (including the
// retired flat envelope), a malformed payload, a blank appointment id, a
// subject that is not the appointment id, an unusable window, no ASN, no event
// time -- which the run loop dead-letters without retrying. Any other error is
// a transient infrastructure failure, after the unit of work rolled back
// (nothing written, claim not recorded), so the same message is retried.
func (c *InboundDemandConsumer) HandleMessage(ctx context.Context, value []byte) error {
	e, err := cloudevents.Decode(value)
	if err != nil {
		return poisonError("not a CloudEvents 1.0 message", err)
	}
	var appt inbound.Appointment
	switch e.Type() {
	case typeDockAppointmentBooked:
		appt, err = c.bookedFrom(e.Subject(), e.Time(), e.DataAs)
	case typeDockAppointmentCancelled:
		appt, err = c.cancelledFrom(e.Subject(), e.Time(), e.DataAs)
	default:
		return nil
	}
	if err != nil {
		return poisonError(fmt.Sprintf("%s %s", e.Type(), e.ID()), err)
	}

	outcome, err := c.Record.Handle(ctx, inboundDemandConsumerName, e.ID(), appt)
	if err != nil {
		return fmt.Errorf("record inbound appointment: %w", err)
	}
	switch outcome {
	case usecases.OutcomeDuplicate:
		c.Logger.InfoContext(ctx, "skipping already-processed inbound event", "event_id", e.ID())
	case usecases.OutcomeStale:
		c.Logger.InfoContext(ctx, "inbound event is older than the stored appointment; model unchanged", "event_id", e.ID(), "appointment_id", appt.ID())
	}
	return nil
}

var errSubjectMismatch = errors.New("subject is not the appointment id")

func (c *InboundDemandConsumer) bookedFrom(subject string, at time.Time, dataAs func(any) error) (inbound.Appointment, error) {
	var data dockAppointmentBookedData
	if err := dataAs(&data); err != nil {
		return inbound.Appointment{}, fmt.Errorf("malformed payload: %w", err)
	}
	if data.AppointmentID != subject {
		return inbound.Appointment{}, errSubjectMismatch
	}
	return inbound.NewBooked(inbound.BookedParams{
		AppointmentID: data.AppointmentID,
		Location:      c.Location,
		WindowStart:   data.WindowStart,
		WindowEnd:     data.WindowEnd,
		ASNCount:      len(data.ASNNumbers),
		AsOf:          at,
	})
}

func (c *InboundDemandConsumer) cancelledFrom(subject string, at time.Time, dataAs func(any) error) (inbound.Appointment, error) {
	var data dockAppointmentCancelledData
	if err := dataAs(&data); err != nil {
		return inbound.Appointment{}, fmt.Errorf("malformed payload: %w", err)
	}
	if data.AppointmentID != subject {
		return inbound.Appointment{}, errSubjectMismatch
	}
	return inbound.NewCancelled(data.AppointmentID, c.Location, at)
}

// Close releases the underlying Kafka reader and dead-letter writer.
func (c *InboundDemandConsumer) Close() error {
	err := c.Reader.Close()
	if c.DLQ != nil {
		err = errors.Join(err, c.DLQ.Close())
	}
	return err
}
