// Package kafka is the outbound Kafka adapter: it encodes CapacityPlan
// domain events into CloudEvents 1.0 outbox messages (Encoder) and writes
// drained outbox rows to the broker (RelaySink). Envelopes are built ONLY
// through internal/adapters/kafka/cloudevents.
package kafka

import (
	"fmt"
	"time"

	"github.com/google/uuid"

	"github.com/claudioed/warehouse-planning/internal/adapters/kafka/cloudevents"
	"github.com/claudioed/warehouse-planning/internal/application/outbox"
	"github.com/claudioed/warehouse-planning/internal/application/ports"
	"github.com/claudioed/warehouse-planning/internal/domain/capacityplan"
)

// Topic is this service's integration-event topic.
const Topic = "warehouse.warehouse-planning.events"

// Entity is the `<entity>` segment of the published CloudEvents types: the
// aggregate that raised the event, lowercase, no separators (fleet
// standard) -- com.warehouse.wes.warehouse-planning.capacityplan.<EventName>.
const Entity = "capacityplan"

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

// Encoder implements ports.EventEncoder: each domain event becomes one
// outbox.Message holding the CloudEvents bytes, the Kafka key (plan id) and
// headers (content-type). The CloudEvents `id` is minted HERE, once, and
// persisted with the outbox row, so a relay retry republishes the same id.
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

// Encode encodes events in order. It fails on an event type it does not
// publish (a programming error: nothing silently goes missing).
func (e *Encoder) Encode(events ...capacityplan.Event) ([]outbox.Message, error) {
	out := make([]outbox.Message, 0, len(events))
	for _, ev := range events {
		data, err := payloadFor(ev)
		if err != nil {
			return nil, err
		}
		id := e.NewID()
		value, err := cloudevents.New(cloudevents.Spec{
			ID:        id,
			Entity:    Entity,
			EventName: ev.EventName(),
			Subject:   ev.AggregateID(),
			Time:      ev.OccurredAt(),
			Stream:    cloudevents.StreamEvents,
			Version:   schemaVersion,
			Data:      data,
		})
		if err != nil {
			return nil, fmt.Errorf("encode %s: %w", ev.EventName(), err)
		}
		ct := cloudevents.ContentTypeHeader()
		out = append(out, outbox.Message{
			EventID:    id,
			Topic:      e.topic(),
			EventType:  cloudevents.Type(Entity, ev.EventName()),
			Subject:    ev.AggregateID(),
			Key:        []byte(ev.AggregateID()),
			DataSchema: cloudevents.DataSchema(cloudevents.StreamEvents, ev.EventName(), schemaVersion),
			Value:      value,
			Headers:    []outbox.Header{{Key: ct.Key, Value: string(ct.Value)}},
		})
	}
	return out, nil
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
			PlanID: e.PlanID, WarehouseID: e.WarehouseID, Location: e.Location, PathID: e.PathID,
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
