// Package kafka is the outbound Kafka adapter: it encodes CapacityPlan and
// ProcessCapacity domain events into CloudEvents 1.0 outbox messages
// (Encoder) and writes drained outbox rows to the broker (RelaySink).
// Envelopes are built ONLY through internal/adapters/kafka/cloudevents.
package kafka

import (
	"fmt"
	"time"

	"github.com/google/uuid"

	"github.com/claudioed/warehouse-planning/internal/adapters/kafka/cloudevents"
	"github.com/claudioed/warehouse-planning/internal/application/outbox"
	"github.com/claudioed/warehouse-planning/internal/application/ports"
	"github.com/claudioed/warehouse-planning/internal/domain/capacityplan"
	"github.com/claudioed/warehouse-planning/internal/domain/processcapacity"
)

// Topic is this service's integration-event topic.
const Topic = "warehouse.warehouse-planning.events"

// Entity is the `<entity>` segment of the published CloudEvents types: the
// aggregate that raised the event, lowercase, no separators (fleet
// standard) -- com.warehouse.wes.warehouse-planning.capacityplan.<EventName>.
const Entity = "capacityplan"

// ProcessCapacityEntity is the entity segment of ProcessCapacity events:
// com.warehouse.wes.warehouse-planning.processcapacity.<EventName>.
const ProcessCapacityEntity = "processcapacity"

// schemaVersion is the dataschema version of every payload below
// (urn:warehouse:warehouse-planning:events:<EventName>:v1). A breaking
// payload change needs a new version, never a mutation.
const schemaVersion = 1

// Wire payloads (CloudEvents `data`, snake_case JSON). Quantities are
// orders; path_capacity is ORDER per HOUR; times are RFC3339 UTC.

type planCreatedData struct {
	PlanID             string    `json:"plan_id"`
	WarehouseID        string    `json:"warehouse_id"`
	Location           string    `json:"location"`
	PathID             string    `json:"path_id"`
	WindowStart        time.Time `json:"window_start"`
	WindowEnd          time.Time `json:"window_end"`
	AssignedDemand     float64   `json:"assigned_demand"`
	PathCapacity       float64   `json:"path_capacity"`
	CapacityOverWindow float64   `json:"capacity_over_window"`
	Shortage           float64   `json:"shortage"`
	BottleneckStep     string    `json:"bottleneck_step"`
	Status             string    `json:"status"`
}

type planPublishedData struct {
	PlanID      string `json:"plan_id"`
	WarehouseID string `json:"warehouse_id"`
	// SiteID is the canonical site (a facility-layout Site site_code) the
	// plan is scoped to. Additive on the v1 payload: omitted when empty
	// (a plan stored before migration 0008), so legacy bytes stay
	// byte-identical and legacy payloads without it still decode.
	SiteID             string    `json:"site_id,omitempty"`
	Location           string    `json:"location"`
	PathID             string    `json:"path_id"`
	WindowStart        time.Time `json:"window_start"`
	WindowEnd          time.Time `json:"window_end"`
	AssignedDemand     float64   `json:"assigned_demand"`
	PathCapacity       float64   `json:"path_capacity"`
	CapacityOverWindow float64   `json:"capacity_over_window"`
	Shortage           float64   `json:"shortage"`
	BottleneckStep     string    `json:"bottleneck_step"`
	PublishedAt        time.Time `json:"published_at"`
}

type shortageDetectedData struct {
	PlanID             string    `json:"plan_id"`
	WarehouseID        string    `json:"warehouse_id"`
	Location           string    `json:"location"`
	PathID             string    `json:"path_id"`
	WindowStart        time.Time `json:"window_start"`
	WindowEnd          time.Time `json:"window_end"`
	AssignedDemand     float64   `json:"assigned_demand"`
	CapacityOverWindow float64   `json:"capacity_over_window"`
	Shortage           float64   `json:"shortage"`
	BottleneckStep     string    `json:"bottleneck_step"`
}

type bottleneckDetectedData struct {
	PlanID         string    `json:"plan_id"`
	WarehouseID    string    `json:"warehouse_id"`
	Location       string    `json:"location"`
	PathID         string    `json:"path_id"`
	WindowStart    time.Time `json:"window_start"`
	WindowEnd      time.Time `json:"window_end"`
	BottleneckStep string    `json:"bottleneck_step"`
	PathCapacity   float64   `json:"path_capacity"`
}

// processCapacityRegisteredData: identity + the native constraint that
// created the aggregate (whose rate IS the effective rate at that moment).
// quantity/unit/period_seconds mirror the REST GET's constraint shape.
type processCapacityRegisteredData struct {
	ProcessType    string    `json:"process_type"`
	Location       string    `json:"location"`
	WindowStart    time.Time `json:"window_start"`
	WindowEnd      time.Time `json:"window_end"`
	ConstraintType string    `json:"constraint_type"`
	Quantity       float64   `json:"quantity"`
	Unit           string    `json:"unit"`
	PeriodSeconds  float64   `json:"period_seconds"`
}

// processCapacityChangedData: identity + the registration that caused the
// change + the effective rate before and after it.
type processCapacityChangedData struct {
	ProcessType    string    `json:"process_type"`
	Location       string    `json:"location"`
	WindowStart    time.Time `json:"window_start"`
	WindowEnd      time.Time `json:"window_end"`
	ConstraintType string    `json:"constraint_type"`

	PreviousQuantity          float64 `json:"previous_quantity"`
	PreviousUnit              string  `json:"previous_unit"`
	PreviousPeriodSeconds     float64 `json:"previous_period_seconds"`
	PreviousBindingConstraint string  `json:"previous_binding_constraint"`

	EffectiveQuantity          float64 `json:"effective_quantity"`
	EffectiveUnit              string  `json:"effective_unit"`
	EffectivePeriodSeconds     float64 `json:"effective_period_seconds"`
	EffectiveBindingConstraint string  `json:"effective_binding_constraint"`
}

// Encoder implements ports.EventEncoder: each domain event becomes one
// outbox.Message holding the CloudEvents bytes, the Kafka key (the
// aggregate id) and headers (content-type). The CloudEvents `id` is minted
// HERE, once, and persisted with the outbox row, so a relay retry
// republishes the same id.
type Encoder struct {
	// NewID mints a CloudEvents id (a UUID v4 string); uuid.NewString when
	// nil. Pinned in golden tests.
	NewID func() string
	// Topic overrides the destination topic (Topic when empty). Production
	// leaves it empty; integration tests give each run a unique topic.
	Topic string
}

// NewEncoder returns an Encoder minting random UUID v4 ids.
func NewEncoder() *Encoder { return &Encoder{NewID: uuid.NewString} }

var _ ports.EventEncoder = (*Encoder)(nil)

// EncodeCapacityPlanEvents implements ports.EventEncoder for CapacityPlan
// events. It encodes in order and fails on an event type it does not
// publish (a programming error: nothing silently goes missing).
func (e *Encoder) EncodeCapacityPlanEvents(events ...capacityplan.Event) ([]outbox.Message, error) {
	out := make([]outbox.Message, 0, len(events))
	for _, ev := range events {
		msg, err := e.encodePlanOne(ev, e.id()())
		if err != nil {
			return nil, err
		}
		out = append(out, msg)
	}
	return out, nil
}

// EncodeProcessCapacityEvents implements ports.EventEncoder for
// ProcessCapacity events, on the same integration topic with the
// processcapacity entity segment in the type.
func (e *Encoder) EncodeProcessCapacityEvents(events ...processcapacity.Event) ([]outbox.Message, error) {
	out := make([]outbox.Message, 0, len(events))
	for _, ev := range events {
		msg, err := e.encodeProcessOne(ev, e.id()())
		if err != nil {
			return nil, err
		}
		out = append(out, msg)
	}
	return out, nil
}

// id returns the id minting function (e.NewID or uuid.NewString).
func (e *Encoder) id() func() string {
	if e.NewID != nil {
		return e.NewID
	}
	return uuid.NewString
}

// encodePlanOne encodes one CapacityPlan occurrence under the given
// CloudEvents id. The id is a parameter so FanoutEncoder can stamp the
// SAME id on the integration and the analytics message of one occurrence.
func (e *Encoder) encodePlanOne(ev capacityplan.Event, id string) (outbox.Message, error) {
	data, err := payloadFor(ev)
	if err != nil {
		return outbox.Message{}, err
	}
	return buildMessage(Entity, ev, id, e.topic(), cloudevents.StreamEvents, data)
}

// encodeProcessOne encodes one ProcessCapacity occurrence under the given
// CloudEvents id (integration stream only: ProcessCapacity events are not
// plan facts and get no analytics copy, ADR 0005).
func (e *Encoder) encodeProcessOne(ev processcapacity.Event, id string) (outbox.Message, error) {
	data, err := processPayloadFor(ev)
	if err != nil {
		return outbox.Message{}, err
	}
	return buildMessage(ProcessCapacityEntity, ev, id, e.topic(), cloudevents.StreamEvents, data)
}

// eventEnvelope is the attributes every encodable event exposes, whatever
// aggregate raised it (both capacityplan.Event and processcapacity.Event
// satisfy it structurally).
type eventEnvelope interface {
	EventName() string
	AggregateID() string
	OccurredAt() time.Time
}

// buildMessage builds the structured-mode CloudEvent and wraps it into the
// outbox row shape; integration and analytics messages differ only in
// topic, stream (hence dataschema) and payload.
func buildMessage(entity string, ev eventEnvelope, id, topic, stream string, data any) (outbox.Message, error) {
	value, err := cloudevents.New(cloudevents.Spec{
		ID:        id,
		Entity:    entity,
		EventName: ev.EventName(),
		Subject:   ev.AggregateID(),
		Time:      ev.OccurredAt(),
		Stream:    stream,
		Version:   schemaVersion,
		Data:      data,
	})
	if err != nil {
		return outbox.Message{}, fmt.Errorf("encode %s: %w", ev.EventName(), err)
	}
	ct := cloudevents.ContentTypeHeader()
	return outbox.Message{
		EventID:    id,
		Topic:      topic,
		EventType:  cloudevents.Type(entity, ev.EventName()),
		Subject:    ev.AggregateID(),
		Key:        []byte(ev.AggregateID()),
		DataSchema: cloudevents.DataSchema(stream, ev.EventName(), schemaVersion),
		Value:      value,
		Headers:    []outbox.Header{{Key: ct.Key, Value: string(ct.Value)}},
	}, nil
}

func (e *Encoder) topic() string {
	if e.Topic != "" {
		return e.Topic
	}
	return Topic
}

func payloadFor(ev capacityplan.Event) (any, error) {
	switch e := ev.(type) {
	case capacityplan.CapacityPlanCreated:
		return planCreatedData{
			PlanID: e.PlanID, WarehouseID: e.WarehouseID, Location: e.Location, PathID: e.PathID,
			WindowStart: e.WindowStart.UTC(), WindowEnd: e.WindowEnd.UTC(),
			AssignedDemand: e.AssignedDemand, PathCapacity: e.PathCapacity,
			CapacityOverWindow: e.CapacityOverWindow, Shortage: e.Shortage,
			BottleneckStep: string(e.BottleneckStep), Status: string(capacityplan.StatusDraft),
		}, nil
	case capacityplan.CapacityPlanPublished:
		return planPublishedData{
			PlanID: e.PlanID, WarehouseID: e.WarehouseID, SiteID: e.SiteID, Location: e.Location, PathID: e.PathID,
			WindowStart: e.WindowStart.UTC(), WindowEnd: e.WindowEnd.UTC(),
			AssignedDemand: e.AssignedDemand, PathCapacity: e.PathCapacity,
			CapacityOverWindow: e.CapacityOverWindow, Shortage: e.Shortage,
			BottleneckStep: string(e.BottleneckStep), PublishedAt: e.At.UTC(),
		}, nil
	case capacityplan.CapacityShortageDetected:
		return shortageDetectedData{
			PlanID: e.PlanID, WarehouseID: e.WarehouseID, Location: e.Location, PathID: e.PathID,
			WindowStart: e.WindowStart.UTC(), WindowEnd: e.WindowEnd.UTC(),
			AssignedDemand: e.AssignedDemand, CapacityOverWindow: e.CapacityOverWindow,
			Shortage: e.Shortage, BottleneckStep: string(e.BottleneckStep),
		}, nil
	case capacityplan.BottleneckDetected:
		return bottleneckDetectedData{
			PlanID: e.PlanID, WarehouseID: e.WarehouseID, Location: e.Location, PathID: e.PathID,
			WindowStart: e.WindowStart.UTC(), WindowEnd: e.WindowEnd.UTC(),
			BottleneckStep: string(e.BottleneckStep), PathCapacity: e.PathCapacity,
		}, nil
	default:
		return nil, fmt.Errorf("kafka encoder: %s is not a published event", ev.EventName())
	}
}

// processPayloadFor maps one ProcessCapacity event to its wire payload.
func processPayloadFor(ev processcapacity.Event) (any, error) {
	switch e := ev.(type) {
	case processcapacity.ProcessCapacityRegistered:
		return processCapacityRegisteredData{
			ProcessType:    string(e.ProcessType),
			Location:       e.Location,
			WindowStart:    e.WindowStart.UTC(),
			WindowEnd:      e.WindowEnd.UTC(),
			ConstraintType: string(e.ConstraintType),
			Quantity:       e.Rate.Quantity(),
			Unit:           string(e.Rate.Unit()),
			PeriodSeconds:  e.Rate.Period().Seconds(),
		}, nil
	case processcapacity.ProcessCapacityChanged:
		return processCapacityChangedData{
			ProcessType:    string(e.ProcessType),
			Location:       e.Location,
			WindowStart:    e.WindowStart.UTC(),
			WindowEnd:      e.WindowEnd.UTC(),
			ConstraintType: string(e.ConstraintType),

			PreviousQuantity:          e.PreviousRate.Quantity(),
			PreviousUnit:              string(e.PreviousRate.Unit()),
			PreviousPeriodSeconds:     e.PreviousRate.Period().Seconds(),
			PreviousBindingConstraint: string(e.PreviousBinding),

			EffectiveQuantity:          e.NewRate.Quantity(),
			EffectiveUnit:              string(e.NewRate.Unit()),
			EffectivePeriodSeconds:     e.NewRate.Period().Seconds(),
			EffectiveBindingConstraint: string(e.NewBinding),
		}, nil
	default:
		return nil, fmt.Errorf("kafka encoder: %s is not a published event", ev.EventName())
	}
}
