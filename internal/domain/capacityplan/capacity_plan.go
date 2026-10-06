// Package capacityplan holds the CapacityPlan aggregate: the demand
// assigned to a warehouse location + planning window, the ProcessPath
// capacity available to serve it, and the resulting shortage. See
// .claude/rules/domain-model.md for the ubiquitous language.
//
// The aggregate performs NO I/O: it is built from an already-computed
// ProcessPathCapacity result (processcapacity.ComputeProcessPathCapacity)
// and from explicit ids/timestamps handed in by the application layer.
package capacityplan

import (
	"errors"
	"math"
	"time"

	"github.com/claudioed/warehouse-planning/internal/domain/processcapacity"
)

// Status is a CapacityPlan's lifecycle state.
type Status string

// The two lifecycle states.
const (
	StatusDraft     Status = "DRAFT"
	StatusPublished Status = "PUBLISHED"
)

// DemandSource says where a plan's assigned demand came from.
type DemandSource string

// The two demand sources. DemandSourceRequest is also what a plan created
// before demand ingestion existed is read back as: every such plan carried
// an explicit assigned_demand.
const (
	// DemandSourceRequest: the caller stated assigned_demand explicitly.
	DemandSourceRequest DemandSource = "request"
	// DemandSourceOrders: assigned_demand was omitted and defaulted from the
	// expected-demand read model fed by order-management's order events.
	DemandSourceOrders DemandSource = "orders"
)

var (
	// ErrAlreadyPublished is returned by Publish on a plan that is already
	// PUBLISHED -- a double publish is never silently accepted (it would
	// re-announce the same shortage to every downstream consumer).
	ErrAlreadyPublished = errors.New("capacityplan: capacity plan is already published")

	// ErrNegativeDemand is returned when assigned demand is below zero.
	// Zero demand is valid (nothing to serve, never a shortage).
	ErrNegativeDemand = errors.New("capacityplan: assigned demand must not be negative")

	// ErrRequiredField is returned when id, warehouse id, site id,
	// location or process path id is blank.
	ErrRequiredField = errors.New("capacityplan: id, warehouse id, site id, location and process path id are required")

	// ErrPathRateNotOrder is returned when the path capacity handed to
	// Create is not an ORDER rate. ComputeProcessPathCapacity always
	// normalizes to ORDER; anything else means the caller skipped it.
	ErrPathRateNotOrder = errors.New("capacityplan: path capacity must be an ORDER rate")
)

// CapacityPlan is the aggregate root, identified by its id (a UUID string).
// Its natural key is (WarehouseID, PlanningWindow) -- see domain-model.md --
// but several plans (e.g. different locations or paths) may exist for it.
type CapacityPlan struct {
	id            string
	warehouseID   string
	siteID        string
	location      string
	window        processcapacity.CapacityWindow
	processPathID string

	assignedDemand float64
	// demandSource is where assignedDemand came from; informational, no
	// published event carries it.
	demandSource DemandSource

	// pathCapacity is the ORDER/HOUR rate of the path (the quantity of
	// the normalized ProcessPathCapacity expressed per hour).
	pathCapacity       float64
	bottleneckStep     processcapacity.ProcessType
	capacityOverWindow float64
	shortage           float64

	// bottleneckConstraint is the constraint type binding the bottleneck
	// step (LABOR, STATION, ...) and warnings are the composition warnings
	// raised when the path capacity was computed. Both are informational
	// read-model fields: no INTEGRATION event carries them; the constraint
	// rides on CapacityPlanPublished for the analytics stream only.
	bottleneckConstraint processcapacity.ConstraintType
	warnings             []string

	status      Status
	createdAt   time.Time
	publishedAt time.Time

	events []Event
}

// CreateParams carries everything Create needs. PathRate and
// BottleneckStep are the results of ComputeProcessPathCapacity.
type CreateParams struct {
	ID          string
	WarehouseID string
	// SiteID is the canonical site the plan is scoped to: a
	// facility-layout Site site_code. Required for new plans (a blank one
	// is ErrRequiredField); never inferred from WarehouseID or Location.
	SiteID         string
	Location       string
	Window         processcapacity.CapacityWindow
	ProcessPathID  string
	AssignedDemand float64
	PathRate       processcapacity.CapacityRate
	BottleneckStep processcapacity.ProcessType

	// DemandSource is where AssignedDemand came from; the zero value means
	// DemandSourceRequest (the demand was stated explicitly).
	DemandSource DemandSource

	// BottleneckConstraint and Warnings come from the composed path
	// capacity (processcapacity.ComposeProcessPathCapacity).
	BottleneckConstraint processcapacity.ConstraintType
	Warnings             []string
}

// Create builds a DRAFT CapacityPlan, computes its derived fields and
// records CapacityPlanCreated (occurring at now).
//
//	pathCapacity       = PathRate expressed per hour (ORDER/HOUR)
//	capacityOverWindow = pathCapacity x window hours
//	shortage           = max(0, demand - capacityOverWindow), never negative
func Create(p CreateParams, now time.Time) (*CapacityPlan, error) {
	if p.ID == "" || p.WarehouseID == "" || p.SiteID == "" || p.Location == "" || p.ProcessPathID == "" {
		return nil, ErrRequiredField
	}
	if p.AssignedDemand < 0 {
		return nil, ErrNegativeDemand
	}
	if p.PathRate.Unit() != processcapacity.UnitOrder {
		return nil, ErrPathRateNotOrder
	}

	perHour := p.PathRate.Quantity() / p.PathRate.Period().Hours()
	over := perHour * p.Window.Duration().Hours()

	plan := &CapacityPlan{
		id:                 p.ID,
		warehouseID:        p.WarehouseID,
		siteID:             p.SiteID,
		location:           p.Location,
		window:             p.Window,
		processPathID:      p.ProcessPathID,
		assignedDemand:     p.AssignedDemand,
		demandSource:       sourceOrRequest(p.DemandSource),
		pathCapacity:       perHour,
		bottleneckStep:     p.BottleneckStep,
		capacityOverWindow: over,
		shortage:           math.Max(0, p.AssignedDemand-over),
		status:             StatusDraft,
		createdAt:          now,

		bottleneckConstraint: p.BottleneckConstraint,
		warnings:             append([]string(nil), p.Warnings...),
	}
	plan.record(CapacityPlanCreated{
		Header:             Header{PlanID: plan.id, At: now},
		WarehouseID:        plan.warehouseID,
		Location:           plan.location,
		PathID:             plan.processPathID,
		WindowStart:        plan.window.Start(),
		WindowEnd:          plan.window.End(),
		AssignedDemand:     plan.assignedDemand,
		PathCapacity:       plan.pathCapacity,
		CapacityOverWindow: plan.capacityOverWindow,
		Shortage:           plan.shortage,
		BottleneckStep:     plan.bottleneckStep,
	})
	return plan, nil
}

// RehydrateParams is the stored state of a plan, used by repositories to
// rebuild the aggregate without re-deriving or re-announcing anything.
type RehydrateParams struct {
	ID          string
	WarehouseID string
	// SiteID: empty for rows stored before migration 0008 (the plan then
	// reports and publishes an empty site id; never inferred).
	SiteID             string
	Location           string
	Window             processcapacity.CapacityWindow
	ProcessPathID      string
	AssignedDemand     float64
	PathCapacity       float64
	BottleneckStep     processcapacity.ProcessType
	CapacityOverWindow float64
	Shortage           float64
	Status             Status
	CreatedAt          time.Time
	PublishedAt        time.Time

	// DemandSource: empty reads back as DemandSourceRequest.
	DemandSource DemandSource

	BottleneckConstraint processcapacity.ConstraintType
	Warnings             []string
}

// Rehydrate rebuilds a CapacityPlan from persisted state. It records no
// events. It is for repository adapters only; new plans go through Create.
func Rehydrate(p RehydrateParams) *CapacityPlan {
	return &CapacityPlan{
		id:                 p.ID,
		warehouseID:        p.WarehouseID,
		siteID:             p.SiteID,
		location:           p.Location,
		window:             p.Window,
		processPathID:      p.ProcessPathID,
		assignedDemand:     p.AssignedDemand,
		demandSource:       sourceOrRequest(p.DemandSource),
		pathCapacity:       p.PathCapacity,
		bottleneckStep:     p.BottleneckStep,
		capacityOverWindow: p.CapacityOverWindow,
		shortage:           p.Shortage,
		status:             p.Status,
		createdAt:          p.CreatedAt,
		publishedAt:        p.PublishedAt,

		bottleneckConstraint: p.BottleneckConstraint,
		warnings:             append([]string(nil), p.Warnings...),
	}
}

// Publish transitions DRAFT -> PUBLISHED at now and records
// CapacityPlanPublished, plus CapacityShortageDetected and
// BottleneckDetected when (and only when) Shortage > 0. It returns
// ErrAlreadyPublished if the plan is already PUBLISHED, recording nothing.
func (c *CapacityPlan) Publish(now time.Time) error {
	if c.status == StatusPublished {
		return ErrAlreadyPublished
	}
	c.status = StatusPublished
	c.publishedAt = now

	header := Header{PlanID: c.id, At: now}
	c.record(CapacityPlanPublished{
		Header:             header,
		WarehouseID:        c.warehouseID,
		SiteID:             c.siteID,
		Location:           c.location,
		PathID:             c.processPathID,
		WindowStart:        c.window.Start(),
		WindowEnd:          c.window.End(),
		AssignedDemand:     c.assignedDemand,
		PathCapacity:       c.pathCapacity,
		CapacityOverWindow: c.capacityOverWindow,
		Shortage:           c.shortage,
		BottleneckStep:     c.bottleneckStep,

		BottleneckConstraint: c.bottleneckConstraint,
	})
	if c.shortage > 0 {
		c.record(CapacityShortageDetected{
			Header:             header,
			WarehouseID:        c.warehouseID,
			Location:           c.location,
			PathID:             c.processPathID,
			WindowStart:        c.window.Start(),
			WindowEnd:          c.window.End(),
			AssignedDemand:     c.assignedDemand,
			CapacityOverWindow: c.capacityOverWindow,
			Shortage:           c.shortage,
			BottleneckStep:     c.bottleneckStep,
		})
		c.record(BottleneckDetected{
			Header:         header,
			WarehouseID:    c.warehouseID,
			Location:       c.location,
			PathID:         c.processPathID,
			WindowStart:    c.window.Start(),
			WindowEnd:      c.window.End(),
			BottleneckStep: c.bottleneckStep,
			PathCapacity:   c.pathCapacity,
		})
	}
	return nil
}

func (c *CapacityPlan) record(e Event) { c.events = append(c.events, e) }

// PullEvents returns the events recorded since the last PullEvents (or
// since construction) and clears them, so each event is handed over
// exactly once.
func (c *CapacityPlan) PullEvents() []Event {
	out := c.events
	c.events = nil
	return out
}

// ID returns the plan's UUID string.
func (c *CapacityPlan) ID() string { return c.id }

// WarehouseID returns the warehouse the plan is for.
func (c *CapacityPlan) WarehouseID() string { return c.warehouseID }

// SiteID returns the canonical site (a facility-layout Site site_code) the
// plan is scoped to; empty for plans stored before migration 0008.
func (c *CapacityPlan) SiteID() string { return c.siteID }

// Location returns the ProcessCapacity location the plan evaluates.
func (c *CapacityPlan) Location() string { return c.location }

// Window returns the planning window.
func (c *CapacityPlan) Window() processcapacity.CapacityWindow { return c.window }

// ProcessPathID returns the id of the ProcessPath evaluated.
func (c *CapacityPlan) ProcessPathID() string { return c.processPathID }

// AssignedDemand returns the demand, in orders, assigned to the window.
func (c *CapacityPlan) AssignedDemand() float64 { return c.assignedDemand }

// DemandSource returns where AssignedDemand came from: DemandSourceRequest
// (stated by the caller) or DemandSourceOrders (defaulted from the
// order-management demand read model).
func (c *CapacityPlan) DemandSource() DemandSource { return c.demandSource }

// sourceOrRequest maps the zero value to DemandSourceRequest.
func sourceOrRequest(s DemandSource) DemandSource {
	if s == "" {
		return DemandSourceRequest
	}
	return s
}

// PathCapacity returns the path's ORDER/HOUR rate.
func (c *CapacityPlan) PathCapacity() float64 { return c.pathCapacity }

// BottleneckStep returns the path step limiting end-to-end flow.
func (c *CapacityPlan) BottleneckStep() processcapacity.ProcessType { return c.bottleneckStep }

// CapacityOverWindow returns PathCapacity x window hours, in orders.
func (c *CapacityPlan) CapacityOverWindow() float64 { return c.capacityOverWindow }

// Shortage returns max(0, demand - capacityOverWindow), in orders.
func (c *CapacityPlan) Shortage() float64 { return c.shortage }

// Status returns DRAFT or PUBLISHED.
func (c *CapacityPlan) Status() Status { return c.status }

// CreatedAt returns when the plan was created.
func (c *CapacityPlan) CreatedAt() time.Time { return c.createdAt }

// PublishedAt returns when the plan was published; the zero time while DRAFT.
func (c *CapacityPlan) PublishedAt() time.Time { return c.publishedAt }

// BottleneckConstraint returns the constraint type binding the bottleneck
// step (e.g. LABOR, STATION); empty for plans created before it was recorded.
func (c *CapacityPlan) BottleneckConstraint() processcapacity.ConstraintType {
	return c.bottleneckConstraint
}

// Warnings returns the composition warnings raised when the path capacity was
// computed (e.g. stations tallied without a declared station standard). The
// result is a copy.
func (c *CapacityPlan) Warnings() []string { return append([]string(nil), c.warnings...) }
