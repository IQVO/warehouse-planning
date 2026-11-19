package workloadforecast

import "time"

// EventWorkloadForecastReceived is the event name; the CloudEvents type is
// com.warehouse.wes.warehouse-planning.workloadforecast.WorkloadForecastReceived
// (docs/adr/0023 decision 1), published on the integration and the
// analytics topic.
const EventWorkloadForecastReceived = "WorkloadForecastReceived"

// Received is raised by New and by every Receive that changed the buckets.
// It is a superset of wes-work-planning's ChargeForecastReceived (which
// carried path_id only).
type Received struct {
	SiteID        string
	PathID        string
	TotalQuantity int64
	BucketCount   int
	ReceivedAt    time.Time
	Version       int64
}

// EventName returns EventWorkloadForecastReceived.
func (Received) EventName() string { return EventWorkloadForecastReceived }

// AggregateID is "site_id/path_id": the Kafka key and the CloudEvents
// subject.
func (e Received) AggregateID() string { return e.SiteID + "/" + e.PathID }

// OccurredAt is the receipt time.
func (e Received) OccurredAt() time.Time { return e.ReceivedAt }
