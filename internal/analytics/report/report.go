// Package report is the self-contained read-model region of the analytics
// read side (ADR 0005): the shapes of the three planning reports, the
// parameter rules (date range), the pure shaping and percentile logic, and
// the writer/reader ports the analytical store implements. It imports no
// other internal package, so the OLTP domain can never leak into the
// projection and the arch test can keep this region isolated.
package report

import (
	"context"
	"time"
)

// Kind is which of the four published capacity-plan events a fact came from.
type Kind string

// The four capacity-plan events the analytics topic carries; the Kind names
// are the CloudEvents event names (the last segment of `type`).
const (
	KindCreated            Kind = "CapacityPlanCreated"
	KindPublished          Kind = "CapacityPlanPublished"
	KindShortageDetected   Kind = "CapacityShortageDetected"
	KindBottleneckDetected Kind = "BottleneckDetected"
)

// PlanEvent is one decoded, validated analytics event ready to project. A
// field a given event does not carry is nil, so the projection only ever
// overwrites what the event actually states.
type PlanEvent struct {
	Kind        Kind
	EventID     string    // CloudEvents id: the idempotency key
	At          time.Time // CloudEvents time: when the event occurred
	PlanID      string
	WarehouseID string
	Location    string

	BottleneckStep    *string
	BindingConstraint *string
	Shortage          *float64
	CreatedAt         *time.Time
	PublishedAt       *time.Time
}

// Projection is the WRITER port: Apply records the event id and folds the
// event into the model in ONE transaction. applied is false when the id was
// already recorded (a replay): nothing changed.
type Projection interface {
	Apply(ctx context.Context, e PlanEvent) (applied bool, err error)
}

// Reader is the READER port. Every method answers for the half-open range
// [From, To): From inclusive, To exclusive. Results are never nil.
type Reader interface {
	BottleneckCounts(ctx context.Context, r Range) ([]BottleneckCount, error)
	ShortageDays(ctx context.Context, r Range) ([]ShortageDay, error)
	ThroughputDays(ctx context.Context, r Range) ([]ThroughputDay, error)
	Latencies(ctx context.Context, r Range) ([]Latency, error)
}

// Site identifies where a plan was made: the planning location (a site code)
// within its warehouse.
type Site struct {
	WarehouseID string `json:"warehouse_id"`
	Location    string `json:"location"`
}

// BottleneckCount is how many PUBLISHED plans in the range had the given
// bottleneck step bound by the given constraint type at the site. An empty
// BindingConstraint means the plan was created before it was recorded.
type BottleneckCount struct {
	Site
	BottleneckStep    string `json:"bottleneck_step"`
	BindingConstraint string `json:"binding_constraint"`
	Plans             int    `json:"plans"`
}

// ShortageDay is one site-day of PUBLISHED plans: all of them, with and
// without shortage, so the rate is meaningful. Day is the UTC calendar day
// of published_at.
type ShortageDay struct {
	Site
	Day               time.Time `json:"day"`
	PlansPublished    int       `json:"plans_published"`
	PlansWithShortage int       `json:"plans_with_shortage"`
	TotalShortage     float64   `json:"total_shortage"`
}

// ThroughputDay is one site-day of plan throughput: plans created that day
// (by created_at) and plans published that day (by published_at).
type ThroughputDay struct {
	Site
	Day            time.Time `json:"day"`
	PlansCreated   int       `json:"plans_created"`
	PlansPublished int       `json:"plans_published"`
}

// Latency is the create-to-publish latency of the plans published in the
// range at a site, in seconds. Only plans whose creation was also observed
// count (Plans).
type Latency struct {
	Site
	Plans         int     `json:"plans"`
	MedianSeconds float64 `json:"median_seconds"`
	P95Seconds    float64 `json:"p95_seconds"`
}
