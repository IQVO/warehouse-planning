package kafka

import (
	"fmt"

	"github.com/google/uuid"

	"github.com/claudioed/warehouse-planning/internal/adapters/kafka/cloudevents"
	"github.com/claudioed/warehouse-planning/internal/application/outbox"
	"github.com/claudioed/warehouse-planning/internal/application/ports"
	"github.com/claudioed/warehouse-planning/internal/domain/capacityplan"
	"github.com/claudioed/warehouse-planning/internal/domain/processcapacity"
)

// AnalyticsTopic is this service's analytics topic: the data product's own
// stream, consumed only by cmd/planning-projector (ADR 0005). Its DLQ is
// AnalyticsTopic + ".dlq".
const AnalyticsTopic = "warehouse.warehouse-planning.analytics"

// analyticsPublishedData is the ANALYTICS payload of CapacityPlanPublished:
// the integration payload plus binding_constraint, the constraint type
// (LABOR, STATION, ...) binding the bottleneck step. The bottleneck-frequency
// report needs it; the integration payload (and its golden test) stays
// byte-identical. Empty for a plan created before the constraint was
// recorded.
type analyticsPublishedData struct {
	planPublishedData
	BindingConstraint string `json:"binding_constraint"`
}

// AnalyticsEncoder encodes the same four capacity-plan events as Encoder
// onto AnalyticsTopic, with dataschema
// urn:warehouse:warehouse-planning:analytics:<EventName>:v1. Alone it mints
// its own ids (golden tests); production uses FanoutEncoder so both
// messages of one occurrence share one id. The analytics stream is plan
// facts only (ADR 0005): EncodeProcessCapacityEvents refuses, loudly,
// rather than silently dropping or duplicating a ProcessCapacity event.
type AnalyticsEncoder struct {
	// NewID mints a CloudEvents id (uuid.NewString when nil).
	NewID func() string
	// Topic overrides AnalyticsTopic (integration tests use a unique one).
	Topic string
}

// EncodeCapacityPlanEvents implements ports.EventEncoder for CapacityPlan
// events: each becomes one analytics message under a freshly minted id.
func (e *AnalyticsEncoder) EncodeCapacityPlanEvents(events ...capacityplan.Event) ([]outbox.Message, error) {
	newID := e.NewID
	if newID == nil {
		newID = uuid.NewString
	}
	out := make([]outbox.Message, 0, len(events))
	for _, ev := range events {
		msg, err := e.encodeOne(ev, newID())
		if err != nil {
			return nil, err
		}
		out = append(out, msg)
	}
	return out, nil
}

// EncodeProcessCapacityEvents implements ports.EventEncoder by refusing:
// ProcessCapacity events are not plan facts and never reach the analytics
// stream. FanoutEncoder never forwards them here; a direct caller gets a
// loud error instead of a silently missing message.
func (e *AnalyticsEncoder) EncodeProcessCapacityEvents(events ...processcapacity.Event) ([]outbox.Message, error) {
	if len(events) == 0 {
		return nil, nil
	}
	return nil, fmt.Errorf("kafka analytics encoder: %s has no analytics payload (plan facts only, ADR 0005)", events[0].EventName())
}

func (e *AnalyticsEncoder) encodeOne(ev capacityplan.Event, id string) (outbox.Message, error) {
	data, err := analyticsPayloadFor(ev)
	if err != nil {
		return outbox.Message{}, err
	}
	return buildMessage(Entity, ev, id, e.topic(), cloudevents.StreamAnalytics, data)
}

func (e *AnalyticsEncoder) topic() string {
	if e.Topic != "" {
		return e.Topic
	}
	return AnalyticsTopic
}

// analyticsPayloadFor is the integration payload plus the analytics-only
// additions (today: binding_constraint on CapacityPlanPublished).
func analyticsPayloadFor(ev capacityplan.Event) (any, error) {
	data, err := payloadFor(ev)
	if err != nil {
		return nil, err
	}
	if e, ok := ev.(capacityplan.CapacityPlanPublished); ok {
		return analyticsPublishedData{
			planPublishedData: data.(planPublishedData),
			BindingConstraint: string(e.BottleneckConstraint),
		}, nil
	}
	return data, nil
}

// FanoutEncoder is the encoder the use cases get: every CapacityPlan event
// becomes TWO outbox messages, the integration one first and the analytics
// one second, carrying the SAME CloudEvents id (minted once here,
// persisted with both rows) and the same `type`. Both rows are inserted by
// the use case's single UnitOfWork, so they commit or roll back together
// with the aggregate; a relay retry republishes each row's persisted bytes.
// A ProcessCapacity event becomes ONE message, the integration one: the
// analytics stream stays plan facts only (ADR 0005).
type FanoutEncoder struct {
	Integration *Encoder
	Analytics   *AnalyticsEncoder
	// NewID mints the shared CloudEvents id (uuid.NewString when nil).
	NewID func() string
}

// NewFanoutEncoder returns a FanoutEncoder over the production topics.
func NewFanoutEncoder() *FanoutEncoder {
	return &FanoutEncoder{Integration: &Encoder{}, Analytics: &AnalyticsEncoder{}, NewID: uuid.NewString}
}

var _ ports.EventEncoder = (*FanoutEncoder)(nil)

// EncodeCapacityPlanEvents encodes plan events in order: integration
// message then analytics message per event. It fails on an event type
// neither stream publishes.
func (f *FanoutEncoder) EncodeCapacityPlanEvents(events ...capacityplan.Event) ([]outbox.Message, error) {
	newID := f.NewID
	if newID == nil {
		newID = uuid.NewString
	}
	out := make([]outbox.Message, 0, 2*len(events))
	for _, ev := range events {
		id := newID()
		integration, err := f.Integration.encodePlanOne(ev, id)
		if err != nil {
			return nil, err
		}
		analytics, err := f.Analytics.encodeOne(ev, id)
		if err != nil {
			return nil, err
		}
		out = append(out, integration, analytics)
	}
	return out, nil
}

// EncodeProcessCapacityEvents encodes ProcessCapacity events onto the
// integration topic only (one message each, no analytics copy).
func (f *FanoutEncoder) EncodeProcessCapacityEvents(events ...processcapacity.Event) ([]outbox.Message, error) {
	return f.Integration.EncodeProcessCapacityEvents(events...)
}
