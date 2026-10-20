package mcp

import (
	"context"
	"time"

	"github.com/modelcontextprotocol/go-sdk/mcp"

	"github.com/claudioed/warehouse-planning/internal/application/usecases"
)

// --- declare_inbound_labour_standard (write) ----------------------------------

type declareInboundLabourStandardInput struct {
	Location              string  `json:"location" jsonschema:"the site code the standard applies to, e.g. SIM1; dock appointments carry no site, so every appointment is attributed to the one configured site"`
	PathID                string  `json:"path_id" jsonschema:"the process path the standard prices, e.g. receive; the same id a capacity plan for that path uses"`
	MinutesPerAppointment float64 `json:"minutes_per_appointment" jsonschema:"fixed labour minutes of ONE dock appointment; must be positive"`
	MinutesPerASN         float64 `json:"minutes_per_asn,omitempty" jsonschema:"OPTIONAL extra labour minutes per ASN the appointment covers (the booked event carries no units, pallets or lines); omit or 0 to use only minutes_per_appointment; must not be negative"`
}

type declareInboundLabourStandardOutput struct {
	Location              string  `json:"location"`
	PathID                string  `json:"path_id"`
	MinutesPerAppointment float64 `json:"minutes_per_appointment"`
	MinutesPerASN         float64 `json:"minutes_per_asn"`
	Created               bool    `json:"created" jsonschema:"true when a new standard was declared, false when an existing one was replaced"`
}

func (d Deps) declareInboundLabourStandard(ctx context.Context, in declareInboundLabourStandardInput) (declareInboundLabourStandardOutput, error) {
	standard, created, err := d.DeclareInboundLabourStandard.Handle(ctx, usecases.DeclareInboundLabourStandardCommand{
		Location: in.Location, ProcessPathID: in.PathID,
		MinutesPerAppointment: in.MinutesPerAppointment, MinutesPerASN: in.MinutesPerASN,
	})
	if err != nil {
		return declareInboundLabourStandardOutput{}, mapError(err)
	}
	return declareInboundLabourStandardOutput{
		Location: standard.Location(), PathID: standard.ProcessPathID(),
		MinutesPerAppointment: standard.MinutesPerAppointment(), MinutesPerASN: standard.MinutesPerASN(),
		Created: created,
	}, nil
}

// --- get_inbound_demand (read) -------------------------------------------------

type getInboundDemandInput struct {
	Location    string `json:"location" jsonschema:"the site code the dock appointments are attributed to, e.g. SIM1"`
	PathID      string `json:"path_id" jsonschema:"the process path whose declared inbound standard prices the appointments, e.g. receive"`
	WindowStart string `json:"window_start" jsonschema:"window start, RFC3339 timestamp; inclusive: an appointment whose door window starts exactly at window_start counts"`
	WindowEnd   string `json:"window_end" jsonschema:"window end, RFC3339 timestamp; exclusive: an appointment starting exactly at window_end belongs to the next window; must be after window_start"`
}

// inboundDemandOutput is identical to the REST GET /inbound-demand body.
type inboundDemandOutput struct {
	Location         string  `json:"location"`
	PathID           string  `json:"path_id"`
	WindowStart      string  `json:"window_start"`
	WindowEnd        string  `json:"window_end"`
	Appointments     int     `json:"appointments"`
	ASNs             int     `json:"asns"`
	LabourMinutes    float64 `json:"labour_minutes"`
	AverageHeads     float64 `json:"average_heads"`
	StandardDeclared bool    `json:"standard_declared"`
	StandardMissing  bool    `json:"standard_missing"`
	Source           string  `json:"source"`
	AsOf             *string `json:"as_of"`
}

func (d Deps) getInboundDemand(ctx context.Context, in getInboundDemandInput) (inboundDemandOutput, error) {
	if in.Location == "" {
		return inboundDemandOutput{}, toolError("missing-location", "location is required")
	}
	if in.PathID == "" {
		return inboundDemandOutput{}, toolError("missing-path-id", "path_id is required")
	}
	start, end, err := parseWindow(in.WindowStart, in.WindowEnd)
	if err != nil {
		return inboundDemandOutput{}, err
	}
	res, err := d.GetInboundDemand.Handle(ctx, in.Location, in.PathID, start, end)
	if err != nil {
		return inboundDemandOutput{}, mapError(err)
	}
	out := inboundDemandOutput{
		Location: in.Location, PathID: in.PathID,
		WindowStart:      start.UTC().Format(time.RFC3339),
		WindowEnd:        end.UTC().Format(time.RFC3339),
		Appointments:     res.Demand.Appointments,
		ASNs:             res.Demand.ASNs,
		LabourMinutes:    res.LabourMinutes,
		AverageHeads:     res.AverageHeads,
		StandardDeclared: res.StandardDeclared,
		StandardMissing:  res.StandardMissing,
		Source:           "inbound-receiving",
	}
	if !res.Demand.AsOf.IsZero() {
		asOf := res.Demand.AsOf.UTC().Format(time.RFC3339)
		out.AsOf = &asOf
	}
	return out, nil
}

// --- get_capacity_plan_variance (read) ----------------------------------------

type getCapacityPlanVarianceInput struct {
	ID            string   `json:"id" jsonschema:"the capacity plan's id, as returned by create_capacity_plan"`
	TasksPerOrder *float64 `json:"tasks_per_order,omitempty" jsonschema:"OPTIONAL conversion factor from completed TASKS to ORDERS for the plan's bottleneck step (fulfillment-execution reports tasks, the plan is in orders); without it the observed side is returned and the comparison says it cannot be computed; must be positive"`
}

// planVarianceOutput is identical to the REST GET /capacity-plans/{id}/variance
// body.
type planVarianceOutput struct {
	PlanID                  string           `json:"plan_id"`
	Location                string           `json:"location"`
	Step                    string           `json:"step"`
	WindowStart             string           `json:"window_start"`
	WindowEnd               string           `json:"window_end"`
	PlannedOrdersPerHour    float64          `json:"planned_orders_per_hour"`
	PlannedOrdersOverWindow float64          `json:"planned_orders_over_window"`
	Observed                observedOutput   `json:"observed"`
	Comparison              comparisonOutput `json:"comparison"`
	Advisory                bool             `json:"advisory"`
}

type observedOutput struct {
	Source          string   `json:"source"`
	HasData         bool     `json:"has_data"`
	Tasks           int64    `json:"tasks"`
	TasksPerHour    float64  `json:"tasks_per_hour"`
	MeanTaskSeconds *float64 `json:"mean_task_seconds"`
	AsOf            *string  `json:"as_of"`
}

type comparisonOutput struct {
	Comparable      bool     `json:"comparable"`
	Reason          string   `json:"reason,omitempty"`
	TasksPerOrder   *float64 `json:"tasks_per_order"`
	ObservedOrders  *float64 `json:"observed_orders"`
	VarianceOrders  *float64 `json:"variance_orders"`
	VariancePercent *float64 `json:"variance_percent"`
}

func (d Deps) getCapacityPlanVariance(ctx context.Context, in getCapacityPlanVarianceInput) (planVarianceOutput, error) {
	v, err := d.GetCapacityPlanVariance.Handle(ctx, in.ID, in.TasksPerOrder)
	if err != nil {
		return planVarianceOutput{}, mapError(err)
	}
	out := planVarianceOutput{
		PlanID: v.PlanID, Location: v.Location, Step: v.Step,
		WindowStart:             v.WindowStart.UTC().Format(time.RFC3339),
		WindowEnd:               v.WindowEnd.UTC().Format(time.RFC3339),
		PlannedOrdersPerHour:    v.PlannedPerHour,
		PlannedOrdersOverWindow: v.PlannedOverWindow,
		Observed: observedOutput{
			Source: "fulfillment-execution", HasData: v.HasData, Tasks: v.Tasks,
			TasksPerHour: v.TasksPerHour, MeanTaskSeconds: v.MeanTaskSeconds,
		},
		Comparison: comparisonOutput{
			Comparable: v.Comparable, Reason: v.Reason, TasksPerOrder: v.TasksPerOrder,
			ObservedOrders: v.ObservedOrders, VarianceOrders: v.VarianceOrders, VariancePercent: v.VariancePercent,
		},
		Advisory: true,
	}
	if !v.AsOf.IsZero() {
		asOf := v.AsOf.UTC().Format(time.RFC3339)
		out.Observed.AsOf = &asOf
	}
	return out, nil
}

// --- registration -------------------------------------------------------------

// registerInboundObservedTools adds the ADR 0017 tools: the inbound labour
// standard, the inbound demand read and the advisory plan variance read. They
// are registered only when their use case is wired, like the REST routes, so a
// Deps built without them advertises exactly the earlier surface.
func (d Deps) registerInboundObservedTools(server *mcp.Server, idempotent func(bool) *mcp.ToolAnnotations, readOnly *mcp.ToolAnnotations) {
	if d.DeclareInboundLabourStandard != nil {
		addTool(server, &mcp.Tool{
			Name: "declare_inbound_labour_standard",
			Description: "Declare the labour a dock appointment costs at a site for a process path: minutes_per_appointment (required, positive) plus an optional minutes_per_asn per ASN the appointment covers. " +
				"The inbound labour minutes are a planning standard owned by this context: without one, dock appointments are recorded but contribute NO demand and create_capacity_plan warns 'inbound standard missing' (no default number is ever guessed). " +
				"Writes the standard; declaring an existing location/path_id replaces it.",
			Annotations: idempotent(true),
		}, d.declareInboundLabourStandard)
	}
	if d.GetInboundDemand != nil {
		addTool(server, &mcp.Tool{
			Name: "get_inbound_demand",
			Description: "Read the inbound demand of a site over a window from the local read model fed by inbound-receiving's DockAppointmentBooked / DockAppointmentCancelled events: " +
				"appointments = BOOKED dock appointments whose door window STARTS in [window_start, window_end) (start inclusive, end exclusive; a cancelled one never counts, whatever the arrival order), asns = ASNs they cover, " +
				"labour_minutes = what the standard declared for path_id attributes to them (0 and standard_missing true when none is declared), average_heads = labour_minutes over the window length, and as_of = the time of the newest dock-appointment event the site's model reflects (null when none). " +
				"Appointments carry no site, so all are attributed to the one configured site. Read-only.",
			Annotations: readOnly,
		}, d.getInboundDemand)
	}
	if d.GetCapacityPlanVariance != nil {
		addTool(server, &mcp.Tool{
			Name: "get_capacity_plan_variance",
			Description: "Compare a stored capacity plan with what the floor actually did: the TASKS completed for the plan's bottleneck step at its location over its window (fulfillment-execution TaskCompleted events, minute resolution) against the plan's ORDER/hour and capacity over the window. " +
				"Advisory only: it never changes a planned capacity or the plan. The comparison needs tasks_per_order (tasks are not orders); without it comparable is false and the reason says so. " +
				"has_data false means nothing was observed, not zero throughput. Read-only.",
			Annotations: readOnly,
		}, d.getCapacityPlanVariance)
	}
}
