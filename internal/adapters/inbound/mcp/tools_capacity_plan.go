package mcp

import (
	"context"
	"time"

	"github.com/claudioed/warehouse-planning/internal/application/usecases"
	"github.com/claudioed/warehouse-planning/internal/domain/capacityplan"
)

type createPlanInput struct {
	WarehouseID      string   `json:"warehouse_id" jsonschema:"the warehouse the plan is for, e.g. WH-1"`
	SiteID           string   `json:"site_id" jsonschema:"REQUIRED canonical site code (a facility-layout Site site_code, e.g. SIM1) the plan is scoped to; never inferred from warehouse_id or location"`
	Location         string   `json:"location" jsonschema:"the location whose step capacities are used, e.g. FC01"`
	WindowStart      string   `json:"window_start" jsonschema:"plan window start, RFC3339 timestamp; a registered capacity window applies to a step when it COVERS [window_start, window_end), not only when equal"`
	WindowEnd        string   `json:"window_end" jsonschema:"plan window end, RFC3339 timestamp; must be after window_start"`
	PathID           string   `json:"path_id" jsonschema:"the registered process path to evaluate"`
	AssignedDemand   *float64 `json:"assigned_demand,omitempty" jsonschema:"OPTIONAL demand assigned to the window, in ORDERS (must not be negative; 0 is a real figure). When given it is used exactly as stated. When omitted, the plan uses the number of orders order-management has promised at this location inside the window (see get_expected_demand) and demand_source is orders; if there are none the call fails with missing-assigned-demand - the demand is never silently zero"`
	UnitsPerOrder    *float64 `json:"units_per_order,omitempty" jsonschema:"workload conversion factor: UNIT per ORDER; required if any step is measured in UNIT"`
	PackagesPerOrder *float64 `json:"packages_per_order,omitempty" jsonschema:"workload conversion factor: PACKAGE per ORDER; required if any step is measured in PACKAGE"`
}

type planIDInput struct {
	ID string `json:"id" jsonschema:"the capacity plan's id, as returned by create_capacity_plan"`
}

// capacityPlanOutput is the CapacityPlan representation all three plan tools
// return, identical to the REST body. Quantities are orders; path_capacity
// is ORDER per HOUR.
type capacityPlanOutput struct {
	ID          string `json:"id"`
	WarehouseID string `json:"warehouse_id"`
	// SiteID is the canonical site (a facility-layout Site site_code);
	// empty for plans stored before migration 0008. Additive.
	SiteID             string  `json:"site_id"`
	Location           string  `json:"location"`
	WindowStart        string  `json:"window_start"`
	WindowEnd          string  `json:"window_end"`
	PathID             string  `json:"path_id"`
	AssignedDemand     float64 `json:"assigned_demand"`
	Status             string  `json:"status"`
	PathCapacity       float64 `json:"path_capacity"`
	BottleneckStep     string  `json:"bottleneck_step"`
	CapacityOverWindow float64 `json:"capacity_over_window"`
	Shortage           float64 `json:"shortage"`
	CreatedAt          string  `json:"created_at"`
	PublishedAt        *string `json:"published_at,omitempty"`

	// DemandSource: "request" (assigned_demand stated by the caller) or
	// "orders" (defaulted from the order-management read model). Additive.
	DemandSource string `json:"demand_source"`

	// BottleneckConstraint (e.g. LABOR, STATION; empty for plans created
	// before it was recorded) and Warnings (never null) are additive.
	BottleneckConstraint string   `json:"bottleneck_constraint"`
	Warnings             []string `json:"warnings"`

	// Inbound labour demand (docs/adr/0017), additive and informational; the
	// REST body's twin. Never summed into assigned_demand (orders).
	InboundAppointments    int     `json:"inbound_appointments"`
	InboundLabourMinutes   float64 `json:"inbound_labour_minutes"`
	InboundAverageHeads    float64 `json:"inbound_average_heads"`
	InboundStandardMissing bool    `json:"inbound_standard_missing"`
}

func toCapacityPlanOutput(p *capacityplan.CapacityPlan) capacityPlanOutput {
	out := capacityPlanOutput{
		ID:                 p.ID(),
		WarehouseID:        p.WarehouseID(),
		SiteID:             p.SiteID(),
		Location:           p.Location(),
		WindowStart:        p.Window().Start().UTC().Format(time.RFC3339),
		WindowEnd:          p.Window().End().UTC().Format(time.RFC3339),
		PathID:             p.ProcessPathID(),
		AssignedDemand:     p.AssignedDemand(),
		DemandSource:       string(p.DemandSource()),
		Status:             string(p.Status()),
		PathCapacity:       p.PathCapacity(),
		BottleneckStep:     string(p.BottleneckStep()),
		CapacityOverWindow: p.CapacityOverWindow(),
		Shortage:           p.Shortage(),
		CreatedAt:          p.CreatedAt().UTC().Format(time.RFC3339),

		BottleneckConstraint: string(p.BottleneckConstraint()),
		Warnings:             nonNilStrings(p.Warnings()),

		InboundAppointments:    p.Inbound().Appointments,
		InboundLabourMinutes:   p.Inbound().LabourMinutes,
		InboundAverageHeads:    p.InboundAverageHeads(),
		InboundStandardMissing: p.Inbound().StandardMissing,
	}
	if !p.PublishedAt().IsZero() {
		publishedAt := p.PublishedAt().UTC().Format(time.RFC3339)
		out.PublishedAt = &publishedAt
	}
	return out
}

func (d Deps) createCapacityPlan(ctx context.Context, in createPlanInput) (capacityPlanOutput, error) {
	windowStart, windowEnd, err := parseWindow(in.WindowStart, in.WindowEnd)
	if err != nil {
		return capacityPlanOutput{}, err
	}

	// The use case saves the plan and inserts its CloudEvents into the
	// transactional outbox in one UnitOfWork; the relay in cmd/api drains it.
	// An omitted assigned_demand is defaulted from the order-management
	// demand read model, or rejected (docs/adr/0004); a stated one wins.
	cmd := usecases.CreateCapacityPlanCommand{
		WarehouseID:      in.WarehouseID,
		SiteID:           in.SiteID,
		Location:         in.Location,
		WindowStart:      windowStart,
		WindowEnd:        windowEnd,
		ProcessPathID:    in.PathID,
		DemandFromOrders: in.AssignedDemand == nil,
		UnitsPerOrder:    in.UnitsPerOrder,
		PackagesPerOrder: in.PackagesPerOrder,
	}
	if in.AssignedDemand != nil {
		cmd.AssignedDemand = *in.AssignedDemand
	}
	plan, err := d.CreateCapacityPlan.Handle(ctx, cmd)
	if err != nil {
		return capacityPlanOutput{}, mapError(err)
	}
	return toCapacityPlanOutput(plan), nil
}

func (d Deps) publishCapacityPlan(ctx context.Context, in planIDInput) (capacityPlanOutput, error) {
	plan, err := d.PublishCapacityPlan.Handle(ctx, in.ID)
	if err != nil {
		return capacityPlanOutput{}, mapError(err)
	}
	return toCapacityPlanOutput(plan), nil
}

func (d Deps) getCapacityPlan(ctx context.Context, in planIDInput) (capacityPlanOutput, error) {
	plan, err := d.CapacityPlans.FindByID(ctx, in.ID)
	if err != nil {
		return capacityPlanOutput{}, mapError(err)
	}
	if plan == nil {
		return capacityPlanOutput{}, mapError(usecases.ErrCapacityPlanNotFound)
	}
	return toCapacityPlanOutput(plan), nil
}
