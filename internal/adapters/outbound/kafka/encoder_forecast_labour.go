package kafka

import (
	"time"

	"github.com/google/uuid"

	"github.com/claudioed/warehouse-planning/internal/adapters/kafka/cloudevents"
	"github.com/claudioed/warehouse-planning/internal/application/outbox"
	"github.com/claudioed/warehouse-planning/internal/domain/labourplan"
	"github.com/claudioed/warehouse-planning/internal/domain/workloadforecast"
)

// WorkloadForecastEntity is the entity segment of WorkloadForecast events:
// com.warehouse.wes.warehouse-planning.workloadforecast.<EventName>
// (docs/adr/0023 decision 1).
const WorkloadForecastEntity = "workloadforecast"

// LabourPlanEntity is the entity segment of LabourPlan events:
// com.warehouse.wes.warehouse-planning.labourplan.<EventName>
// (docs/adr/0023 decisions 2 and 3).
const LabourPlanEntity = "labourplan"

// Wire payloads of the requirement-side events (docs/adr/0023). Each is the
// SAME on the integration and the analytics stream (no analytics-only
// field); only the topic and the dataschema stream differ. Counts are whole
// numbers, rate and hours JSON numbers, times RFC 3339 UTC.

type workloadForecastReceivedData struct {
	SiteID        string    `json:"site_id"`
	PathID        string    `json:"path_id"`
	TotalQuantity int64     `json:"total_quantity"`
	BucketCount   int       `json:"bucket_count"`
	ReceivedAt    time.Time `json:"received_at"`
	Version       int64     `json:"version"`
}

type labourPlanCommittedData struct {
	SiteID            string  `json:"site_id"`
	ShiftID           string  `json:"shift_id"`
	PathID            string  `json:"path_id"`
	PlannedHeads      int     `json:"planned_heads"`
	RateUnitsPerHour  float64 `json:"rate_units_per_hour"`
	Hours             float64 `json:"hours"`
	InstalledStations int     `json:"installed_stations"`
	// ShiftStart is omitted entirely (never null) when the plan has none.
	ShiftStart  *time.Time `json:"shift_start,omitempty"`
	HeadsSource string     `json:"heads_source"`
	Version     int64      `json:"version"`
}

// labourPlanDriftDetectedData: drift_heads = committed_heads - planned_heads.
type labourPlanDriftDetectedData struct {
	SiteID         string `json:"site_id"`
	ShiftID        string `json:"shift_id"`
	PathID         string `json:"path_id"`
	PlannedHeads   int    `json:"planned_heads"`
	CommittedHeads int    `json:"committed_heads"`
	DriftHeads     int    `json:"drift_heads"`
}

// EncodeWorkloadForecastEvents implements ports.EventEncoder: one
// integration message per WorkloadForecastReceived.
func (e *Encoder) EncodeWorkloadForecastEvents(events ...workloadforecast.Received) ([]outbox.Message, error) {
	return encodeOnStreams(WorkloadForecastEntity, e.id(), forecastReceivedPayload, events, e.stream())
}

// EncodeLabourPlanCommitted implements ports.EventEncoder: one integration
// message per LabourPlanCommitted.
func (e *Encoder) EncodeLabourPlanCommitted(events ...labourplan.Committed) ([]outbox.Message, error) {
	return encodeOnStreams(LabourPlanEntity, e.id(), labourPlanCommittedPayload, events, e.stream())
}

// EncodeLabourPlanDriftDetected implements ports.EventEncoder: one
// integration message per LabourPlanDriftDetected.
func (e *Encoder) EncodeLabourPlanDriftDetected(events ...labourplan.DriftDetected) ([]outbox.Message, error) {
	return encodeOnStreams(LabourPlanEntity, e.id(), labourPlanDriftPayload, events, e.stream())
}

// EncodeWorkloadForecastEvents implements ports.EventEncoder: one analytics
// message per WorkloadForecastReceived, under a freshly minted id.
func (e *AnalyticsEncoder) EncodeWorkloadForecastEvents(events ...workloadforecast.Received) ([]outbox.Message, error) {
	return encodeOnStreams(WorkloadForecastEntity, idOrUUID(e.NewID), forecastReceivedPayload, events, e.stream())
}

// EncodeLabourPlanCommitted implements ports.EventEncoder: one analytics
// message per LabourPlanCommitted, under a freshly minted id.
func (e *AnalyticsEncoder) EncodeLabourPlanCommitted(events ...labourplan.Committed) ([]outbox.Message, error) {
	return encodeOnStreams(LabourPlanEntity, idOrUUID(e.NewID), labourPlanCommittedPayload, events, e.stream())
}

// EncodeLabourPlanDriftDetected implements ports.EventEncoder: one analytics
// message per LabourPlanDriftDetected, under a freshly minted id.
func (e *AnalyticsEncoder) EncodeLabourPlanDriftDetected(events ...labourplan.DriftDetected) ([]outbox.Message, error) {
	return encodeOnStreams(LabourPlanEntity, idOrUUID(e.NewID), labourPlanDriftPayload, events, e.stream())
}

// EncodeWorkloadForecastEvents encodes each WorkloadForecastReceived as TWO
// messages sharing one id: the integration one, then the analytics copy.
func (f *FanoutEncoder) EncodeWorkloadForecastEvents(events ...workloadforecast.Received) ([]outbox.Message, error) {
	return encodeOnStreams(WorkloadForecastEntity, idOrUUID(f.NewID), forecastReceivedPayload, events, f.streams()...)
}

// EncodeLabourPlanCommitted encodes each LabourPlanCommitted as TWO messages
// sharing one id: the integration one, then the analytics copy.
func (f *FanoutEncoder) EncodeLabourPlanCommitted(events ...labourplan.Committed) ([]outbox.Message, error) {
	return encodeOnStreams(LabourPlanEntity, idOrUUID(f.NewID), labourPlanCommittedPayload, events, f.streams()...)
}

// EncodeLabourPlanDriftDetected encodes each LabourPlanDriftDetected as TWO
// messages sharing one id: the integration one, then the analytics copy.
func (f *FanoutEncoder) EncodeLabourPlanDriftDetected(events ...labourplan.DriftDetected) ([]outbox.Message, error) {
	return encodeOnStreams(LabourPlanEntity, idOrUUID(f.NewID), labourPlanDriftPayload, events, f.streams()...)
}

// stream is one destination of an encoded occurrence: its topic and the
// dataschema stream segment (events or analytics).
type stream struct{ topic, name string }

func (e *Encoder) stream() stream { return stream{topic: e.topic(), name: cloudevents.StreamEvents} }

func (e *AnalyticsEncoder) stream() stream {
	return stream{topic: e.topic(), name: cloudevents.StreamAnalytics}
}

// streams is the fan-out order: the integration message first, then its
// analytics copy (the CapacityPlan fan-out order).
func (f *FanoutEncoder) streams() []stream {
	return []stream{f.Integration.stream(), f.Analytics.stream()}
}

// encodeOnStreams encodes the events in order, minting ONE id per occurrence
// and building one message per stream under it, so the integration message
// and its analytics copy share their CloudEvents id. Every stream carries
// the same payload.
func encodeOnStreams[E eventEnvelope](entity string, newID func() string, payload func(E) any, events []E, streams ...stream) ([]outbox.Message, error) {
	out := make([]outbox.Message, 0, len(streams)*len(events))
	for _, ev := range events {
		id, data := newID(), payload(ev)
		for _, s := range streams {
			msg, err := buildMessage(entity, ev, id, s.topic, s.name, data)
			if err != nil {
				return nil, err
			}
			out = append(out, msg)
		}
	}
	return out, nil
}

// idOrUUID returns newID, or uuid.NewString when it is nil.
func idOrUUID(newID func() string) func() string {
	if newID != nil {
		return newID
	}
	return uuid.NewString
}

func forecastReceivedPayload(e workloadforecast.Received) any {
	return workloadForecastReceivedData{
		SiteID: e.SiteID, PathID: e.PathID,
		TotalQuantity: e.TotalQuantity, BucketCount: e.BucketCount,
		ReceivedAt: e.ReceivedAt.UTC(), Version: e.Version,
	}
}

func labourPlanCommittedPayload(e labourplan.Committed) any {
	d := labourPlanCommittedData{
		SiteID: e.SiteID, ShiftID: e.ShiftID, PathID: e.PathID,
		PlannedHeads: e.PlannedHeads, RateUnitsPerHour: e.RateUnitsPerHour, Hours: e.Hours,
		InstalledStations: e.InstalledStations, HeadsSource: string(e.HeadsSource), Version: e.Version,
	}
	if e.ShiftStart != nil {
		start := e.ShiftStart.UTC()
		d.ShiftStart = &start
	}
	return d
}

func labourPlanDriftPayload(e labourplan.DriftDetected) any {
	return labourPlanDriftDetectedData{
		SiteID: e.SiteID, ShiftID: e.ShiftID, PathID: e.PathID,
		PlannedHeads: e.PlannedHeads, CommittedHeads: e.CommittedHeads, DriftHeads: e.DriftHeads,
	}
}
