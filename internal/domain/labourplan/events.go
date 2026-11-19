package labourplan

import "time"

// Event names; the CloudEvents types are
// com.warehouse.wes.warehouse-planning.labourplan.<name> (docs/adr/0023
// decisions 2 and 3), each published on the integration and the analytics
// topic.
const (
	EventLabourPlanCommitted     = "LabourPlanCommitted"
	EventLabourPlanDriftDetected = "LabourPlanDriftDetected"
)

// Committed is raised by New and by every Revise that changed the plan. It
// carries every field of the plan; workforce-management keeps a copy of it
// (its ADR 0043) and applies a message only when Version is greater than the
// stored one.
type Committed struct {
	SiteID            string
	ShiftID           string
	PathID            string
	PlannedHeads      int
	RateUnitsPerHour  float64
	Hours             float64
	InstalledStations int
	ShiftStart        *time.Time
	HeadsSource       HeadsSource
	Version           int64
	At                time.Time
}

// EventName returns EventLabourPlanCommitted.
func (Committed) EventName() string { return EventLabourPlanCommitted }

// AggregateID is "site_id/shift_id/path_id": the Kafka key and the
// CloudEvents subject.
func (e Committed) AggregateID() string { return key(e.SiteID, e.ShiftID, e.PathID) }

// OccurredAt is the commit time.
func (e Committed) OccurredAt() time.Time { return e.At }

// DriftDetected is raised when the heads workforce-management committed for
// a (site, shift, path) differ from this context's labour plan, and the
// difference is new (see Drift.ShouldRaise). It never corrects either side.
type DriftDetected struct {
	SiteID         string
	ShiftID        string
	PathID         string
	PlannedHeads   int
	CommittedHeads int
	DriftHeads     int
	At             time.Time
}

// EventName returns EventLabourPlanDriftDetected.
func (DriftDetected) EventName() string { return EventLabourPlanDriftDetected }

// AggregateID is "site_id/shift_id/path_id", the labour plan's key.
func (e DriftDetected) AggregateID() string { return key(e.SiteID, e.ShiftID, e.PathID) }

// OccurredAt is the detection time.
func (e DriftDetected) OccurredAt() time.Time { return e.At }

// key is the labour plan's key, the path id in its canonical form.
func key(site, shift, path string) string { return site + "/" + shift + "/" + CanonicalPathID(path) }
