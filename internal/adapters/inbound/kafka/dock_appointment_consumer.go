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

// DockSchedulingTopic is dock-scheduling's integration topic. This service
// knows nothing else about that context beyond this topic name and the
// CloudEvents type/payload shapes below, pinned by the yard-management
// contracts and documented in apis/asyncapi.yaml (docs/adr/0020). The retired
// source, inbound-receiving's warehouse.inbound-receiving.events, is no longer
// read.
const DockSchedulingTopic = "warehouse.dock-scheduling.events"

// dock-scheduling's `type` strings. They are the ONLY types this consumer acts
// on; the full string is matched, never a suffix:
//
//   - DockAppointmentBooked: a door window was booked (direction, site, ASNs).
//   - DockAppointmentRescheduled: the booked window moved.
//   - DockAppointmentCancelled: the appointment was cancelled (terminal).
//   - DockAppointmentNoShow: the carrier did not arrive (terminal).
//
// The old inbound-receiving types
// (com.warehouse.wms.inbound-receiving.dockappointment.*) fall through to the
// default branch like any foreign type: they are not consumed, and the
// deployment that would still select them is refused at boot (cmd/api).
// DockAppointmentCheckedIn / DoorReassigned / Completed / AtRisk are
// analytics-only and never reach this topic; door-calendar events on it are
// ignored.
const (
	typeDockAppointmentBooked      = "com.warehouse.wms.dock-scheduling.dockappointment.DockAppointmentBooked"
	typeDockAppointmentRescheduled = "com.warehouse.wms.dock-scheduling.dockappointment.DockAppointmentRescheduled"
	typeDockAppointmentCancelled   = "com.warehouse.wms.dock-scheduling.dockappointment.DockAppointmentCancelled"
	typeDockAppointmentNoShow      = "com.warehouse.wms.dock-scheduling.dockappointment.DockAppointmentNoShow"
)

// dockAppointmentConsumerName namespaces this consumer's rows in
// ports.ProcessedEventRepository. It differs from the retired
// "inbound-demand-consumer" so a claim made under the old source can never
// swallow a dock-scheduling event of the same CloudEvents id.
const dockAppointmentConsumerName = "dock-appointment-consumer"

// dockAppointmentBookedData mirrors (the subset of) dock-scheduling's
// DockAppointmentBooked `data` this service reads -- hand-mirrored, never
// imported from that service's Go types.
type dockAppointmentBookedData struct {
	AppointmentID string            `json:"appointment_id"`
	SiteID        string            `json:"site_id"`
	Direction     inbound.Direction `json:"direction"`
	WindowStart   time.Time         `json:"window_start"`
	WindowEnd     time.Time         `json:"window_end"`
	ASNNumbers    []string          `json:"asn_numbers"`
	Version       int64             `json:"version"`
}

// dockAppointmentRescheduledData mirrors DockAppointmentRescheduled `data`.
// previous_window_* and door_code are ignored.
type dockAppointmentRescheduledData struct {
	AppointmentID string    `json:"appointment_id"`
	SiteID        string    `json:"site_id"`
	WindowStart   time.Time `json:"window_start"`
	WindowEnd     time.Time `json:"window_end"`
	Version       int64     `json:"version"`
}

// dockAppointmentCancelledData mirrors DockAppointmentCancelled `data`; reason
// and door_code are ignored.
type dockAppointmentCancelledData struct {
	AppointmentID string `json:"appointment_id"`
	SiteID        string `json:"site_id"`
	Version       int64  `json:"version"`
}

// dockAppointmentNoShowData mirrors DockAppointmentNoShow `data`; door_code,
// carrier_scac and window_end are ignored.
type dockAppointmentNoShowData struct {
	AppointmentID string            `json:"appointment_id"`
	SiteID        string            `json:"site_id"`
	Direction     inbound.Direction `json:"direction"`
	Version       int64             `json:"version"`
}

// DockAppointmentConsumer maintains the inbound-demand read model from
// dock-scheduling's DockAppointmentBooked / Rescheduled / Cancelled / NoShow
// events. Every appointment is attributed to the `site_id` of its event
// (docs/adr/0020), which closes docs/adr/0017's no-site limitation: there is
// no configured site on this path any more.
//
// Delivery is at-least-once with an atomic effect: the processed-event claim
// and the version-guarded write are ONE unit of work
// (usecases.RecordInboundAppointment) and the offset is committed only after
// HandleMessage returned nil (or the message was dead-lettered). A consumer
// that applies a sibling's events raises no event of its own (no loop).
type DockAppointmentConsumer struct {
	Reader Reader
	Record *usecases.RecordInboundAppointment
	Logger *slog.Logger
	Retry  RetryPolicy

	// DLQ is this consumer's dead-letter writer (topic DockSchedulingTopic +
	// DLQSuffix). A deterministic failure is published there at once; a
	// transient failure is retried domainMaxHandlerAttempts times first.
	DLQ      DeadLetterWriter
	DLQTopic string

	sleep sleepFunc // test hook; nil => real, ctx-cancellable sleep
}

// NewDockAppointmentConsumer constructs a DockAppointmentConsumer reading
// DockSchedulingTopic from brokers under groupID (an env-configured value
// supplied by the composition root -- never a literal, see
// TestKafkaConsumerGroupNeverHardcodedInline). A group without a committed
// offset starts from the earliest retained message, so a fresh deployment
// rebuilds the model from history.
func NewDockAppointmentConsumer(
	brokers []string,
	groupID string,
	record *usecases.RecordInboundAppointment,
	logger *slog.Logger,
) *DockAppointmentConsumer {
	return &DockAppointmentConsumer{
		Reader:   kafkago.NewReader(readerConfig(brokers, DockSchedulingTopic, groupID)),
		Record:   record,
		Logger:   defaultLogger(logger),
		DLQ:      newDomainDLQWriter(brokers),
		DLQTopic: DockSchedulingTopic + DLQSuffix,
	}
}

// Run consumes DockSchedulingTopic until ctx is cancelled or the reader fails, with
// the same at-least-once loop as the other consumers: commit only after
// success, retry the SAME message with capped backoff on a transient error up
// to domainMaxHandlerAttempts times, and dead-letter a deterministic failure at
// once.
func (c *DockAppointmentConsumer) Run(ctx context.Context) error {
	loop := consumeLoop{
		reader: c.Reader,
		handle: func(ctx context.Context, msg kafkago.Message) error { return c.HandleMessage(ctx, msg.Value) },
		logger: defaultLogger(c.Logger),
		name:   "dock appointment consumer",
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

// HandleMessage decodes one CloudEvents 1.0 message and, if it is one of the
// four dock-scheduling appointment types with a usable payload, applies it to
// the read model.
//
// It returns nil for an unrecognized `type` (the topic is shared with door
// calendar events; the retired inbound-receiving types land here too) and for
// a duplicate event id. It returns an error wrapping errPoison for a
// DETERMINISTIC failure -- not CloudEvents 1.0 (including the retired flat
// envelope), a malformed payload, a blank appointment id or site_id, a subject
// that is not the appointment id, an unknown direction, an unusable window, no
// ASN on an INBOUND booking, no aggregate version, no event time -- which the
// run loop dead-letters without retrying. Any other error is a transient
// infrastructure failure, after the unit of work rolled back (nothing written,
// claim not recorded), so the same message is retried.
func (c *DockAppointmentConsumer) HandleMessage(ctx context.Context, value []byte) error {
	e, err := cloudevents.Decode(value)
	if err != nil {
		return poisonError("not a CloudEvents 1.0 message", err)
	}
	var appt inbound.Appointment
	switch e.Type() {
	case typeDockAppointmentBooked:
		appt, err = bookedFrom(e.Subject(), e.Time(), e.DataAs)
	case typeDockAppointmentRescheduled:
		appt, err = rescheduledFrom(e.Subject(), e.Time(), e.DataAs)
	case typeDockAppointmentCancelled:
		appt, err = cancelledFrom(e.Subject(), e.Time(), e.DataAs)
	case typeDockAppointmentNoShow:
		appt, err = noShowFrom(e.Subject(), e.Time(), e.DataAs)
	default:
		return nil
	}
	if err != nil {
		return poisonError(fmt.Sprintf("%s %s", e.Type(), e.ID()), err)
	}

	outcome, err := c.Record.Handle(ctx, dockAppointmentConsumerName, e.ID(), appt)
	if err != nil {
		return fmt.Errorf("record dock appointment: %w", err)
	}
	switch outcome {
	case usecases.OutcomeDuplicate:
		c.Logger.InfoContext(ctx, "skipping already-processed dock-scheduling event", "event_id", e.ID())
	case usecases.OutcomeStale:
		c.Logger.InfoContext(ctx, "dock-scheduling event is not newer than the stored appointment; model unchanged", "event_id", e.ID(), "appointment_id", appt.ID(), "version", appt.Version())
	}
	return nil
}

var errSubjectMismatch = errors.New("subject is not the appointment id")

func bookedFrom(subject string, at time.Time, dataAs func(any) error) (inbound.Appointment, error) {
	var data dockAppointmentBookedData
	if err := dataAs(&data); err != nil {
		return inbound.Appointment{}, fmt.Errorf("malformed payload: %w", err)
	}
	if data.AppointmentID != subject {
		return inbound.Appointment{}, errSubjectMismatch
	}
	return inbound.NewBooked(inbound.BookedParams{
		AppointmentID: data.AppointmentID,
		Location:      data.SiteID,
		Direction:     data.Direction,
		WindowStart:   data.WindowStart,
		WindowEnd:     data.WindowEnd,
		ASNCount:      len(data.ASNNumbers),
		Version:       data.Version,
		AsOf:          at,
	})
}

func rescheduledFrom(subject string, at time.Time, dataAs func(any) error) (inbound.Appointment, error) {
	var data dockAppointmentRescheduledData
	if err := dataAs(&data); err != nil {
		return inbound.Appointment{}, fmt.Errorf("malformed payload: %w", err)
	}
	if data.AppointmentID != subject {
		return inbound.Appointment{}, errSubjectMismatch
	}
	return inbound.NewRescheduled(data.AppointmentID, data.SiteID, data.WindowStart, data.WindowEnd, data.Version, at)
}

func cancelledFrom(subject string, at time.Time, dataAs func(any) error) (inbound.Appointment, error) {
	var data dockAppointmentCancelledData
	if err := dataAs(&data); err != nil {
		return inbound.Appointment{}, fmt.Errorf("malformed payload: %w", err)
	}
	if data.AppointmentID != subject {
		return inbound.Appointment{}, errSubjectMismatch
	}
	return inbound.NewCancelled(data.AppointmentID, data.SiteID, data.Version, at)
}

func noShowFrom(subject string, at time.Time, dataAs func(any) error) (inbound.Appointment, error) {
	var data dockAppointmentNoShowData
	if err := dataAs(&data); err != nil {
		return inbound.Appointment{}, fmt.Errorf("malformed payload: %w", err)
	}
	if data.AppointmentID != subject {
		return inbound.Appointment{}, errSubjectMismatch
	}
	return inbound.NewNoShow(data.AppointmentID, data.SiteID, data.Direction, data.Version, at)
}

// Close releases the underlying Kafka reader and dead-letter writer.
func (c *DockAppointmentConsumer) Close() error {
	err := c.Reader.Close()
	if c.DLQ != nil {
		err = errors.Join(err, c.DLQ.Close())
	}
	return err
}
