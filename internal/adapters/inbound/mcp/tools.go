package mcp

import (
	"context"
	"time"

	"github.com/modelcontextprotocol/go-sdk/mcp"

	"github.com/claudioed/warehouse-planning/internal/application/ports"
	"github.com/claudioed/warehouse-planning/internal/application/usecases"
)

// Deps is everything the MCP tools need, injected by the composition root.
// It carries the SAME use cases (and, for the two plain reads, the same
// repository ports) the REST adapter uses; the adapter never constructs an
// outbound adapter itself.
type Deps struct {
	RegisterProcessCapacityConstraint *usecases.RegisterProcessCapacityConstraint
	// ProcessCapacities backs get_effective_process_capacity. As in the REST
	// adapter the read is a direct, no-invariant repository lookup -- there
	// is no GetEffectiveProcessCapacity use case.
	ProcessCapacities ports.ProcessCapacityRepository

	RegisterProcessPath    *usecases.RegisterProcessPath
	GetProcessPathCapacity *usecases.GetProcessPathCapacity

	CreateCapacityPlan  *usecases.CreateCapacityPlan
	PublishCapacityPlan *usecases.PublishCapacityPlan
	// CapacityPlans backs get_capacity_plan (direct repository read, as REST).
	CapacityPlans ports.CapacityPlanRepository

	// Station capacity (ADR 0002): DeclareStationStandard backs
	// declare_station_standard, StationStandards backs list_station_standards
	// (direct repository read, as REST) and GetStorageCapacity backs
	// get_storage_capacity.
	DeclareStationStandard *usecases.DeclareStationStandard
	StationStandards       ports.StationStandardRepository
	GetStorageCapacity     *usecases.GetStorageCapacity

	// GetExpectedDemand backs get_expected_demand (docs/adr/0004): the
	// expected-demand read model fed by order-management's order events.
	GetExpectedDemand *usecases.GetExpectedDemand

	// Inbound labour demand and observed capacity (docs/adr/0017):
	// DeclareInboundLabourStandard backs declare_inbound_labour_standard,
	// GetInboundDemand backs get_inbound_demand and GetCapacityPlanVariance
	// backs get_capacity_plan_variance.
	DeclareInboundLabourStandard *usecases.DeclareInboundLabourStandard
	GetInboundDemand             *usecases.GetInboundDemand
	GetCapacityPlanVariance      *usecases.GetCapacityPlanVariance
}

// --- shared helpers -----------------------------------------------------------

// parseWindow parses the RFC3339 window bounds every tool takes.
func parseWindow(start, end string) (time.Time, time.Time, error) {
	windowStart, err := time.Parse(time.RFC3339, start)
	if err != nil {
		return time.Time{}, time.Time{}, toolError("malformed-window-start", "window_start must be an RFC3339 timestamp: "+err.Error())
	}
	windowEnd, err := time.Parse(time.RFC3339, end)
	if err != nil {
		return time.Time{}, time.Time{}, toolError("malformed-window-end", "window_end must be an RFC3339 timestamp: "+err.Error())
	}
	return windowStart, windowEnd, nil
}

// --- register_process_capacity_constraint (write) -----------------------------

// --- get_effective_process_capacity (read) ------------------------------------

// --- register_process_path (write) --------------------------------------------

// --- get_process_path_capacity (read) -----------------------------------------

// --- create_capacity_plan (write) ---------------------------------------------

// --- publish_capacity_plan (write) --------------------------------------------

// --- get_capacity_plan (read) -------------------------------------------------

// nonNilStrings returns s, or an empty (non-nil) slice, so a list field
// always serializes as [] and never null.
func nonNilStrings(s []string) []string {
	if s == nil {
		return []string{}
	}
	return s
}

// --- declare_station_standard (write) -----------------------------------------

// --- list_station_standards (read) --------------------------------------------

// --- get_storage_capacity (read) ----------------------------------------------

// --- get_expected_demand (read) -----------------------------------------------

// --- registration -------------------------------------------------------------

// registerTools adds every tool to the server. Read tools are annotated
// read-only; write tools are annotated destructive (non-read-only,
// non-idempotent where a repeat call creates or changes state) so a host can
// see they change state before letting a model call them.
func (d Deps) registerTools(server *mcp.Server) {
	destructive := true
	idempotent := func(v bool) *mcp.ToolAnnotations {
		return &mcp.ToolAnnotations{ReadOnlyHint: false, DestructiveHint: &destructive, IdempotentHint: v}
	}
	readOnly := &mcp.ToolAnnotations{ReadOnlyHint: true}

	addTool(server, &mcp.Tool{
		Name: "register_process_capacity_constraint",
		Description: "Register one capacity constraint (e.g. LABOR 4000 UNIT per hour) for a process step at a location over an exact time window, " +
			"and return the resulting effective rate (the minimum across the step's constraints) and which constraint binds. " +
			"Writes to the ProcessCapacity store; registering again adds another constraint of the same type to the same process/location/window.",
		Annotations: idempotent(false),
	}, d.registerProcessCapacityConstraint)

	addTool(server, &mcp.Tool{
		Name: "get_effective_process_capacity",
		Description: "Read the effective capacity rate of a process step at a location for an exact window, the binding constraint and every registered constraint. " +
			"Read-only; fails with process-capacity-not-found if nothing is registered for exactly that process/location/window.",
		Annotations: readOnly,
	}, d.getEffectiveProcessCapacity)

	addTool(server, &mcp.Tool{
		Name: "register_process_path",
		Description: "Declare a process path: an id, a name and an ordered, non-empty list of step process types (e.g. PICK, REBIN, PACK). " +
			"Writes the ProcessPath read model; re-registering an existing id replaces its name and steps.",
		Annotations: idempotent(true),
	}, d.registerProcessPath)

	addTool(server, &mcp.Tool{
		Name: "get_process_path_capacity",
		Description: "Compute a registered process path's end-to-end capacity at a location and window, normalized to ORDER per hour, and the bottleneck step. " +
			"The window is matched by coverage: a registered constraint window applies when it covers the requested window (the newest window start wins per constraint type; other types still apply). " +
			"Each step's capacity is the minimum of its covering registered constraints and a derived STATION constraint (stations tallied at the site x the declared station standard); step_breakdown reports each step's rate and binding constraint, " +
			"warnings flags stations tallied without a declared standard. Every step needs a registered or derived capacity; units_per_order / packages_per_order convert UNIT and PACKAGE steps. Read-only.",
		Annotations: readOnly,
	}, d.getProcessPathCapacity)

	addTool(server, &mcp.Tool{
		Name: "create_capacity_plan",
		Description: "Evaluate assigned demand (orders) against a process path's capacity over a window and store a DRAFT capacity plan, returning " +
			"path capacity (ORDER/hour), capacity over the window, shortage (orders, 0 if none), the bottleneck step and demand_source. " +
			"assigned_demand is optional: when omitted it defaults to the orders order-management expects at the location in the window (get_expected_demand; demand_source orders) and fails with missing-assigned-demand when there are none; a stated value always wins (demand_source request). " +
			"The window is matched by coverage: a registered constraint window applies to a step when it covers the plan window (newest window start wins per constraint type). " +
			"Writes the plan and queues its integration events in the transactional outbox; each call creates a new plan.",
		Annotations: idempotent(false),
	}, d.createCapacityPlan)

	addTool(server, &mcp.Tool{
		Name: "publish_capacity_plan",
		Description: "Publish a DRAFT capacity plan, making it PUBLISHED and queueing its CapacityPlanPublished (and shortage/bottleneck) events in the outbox. " +
			"Irreversible and allowed once per plan: publishing again fails with capacity-plan-already-published; an unknown id fails with capacity-plan-not-found.",
		Annotations: idempotent(false),
	}, d.publishCapacityPlan)

	addTool(server, &mcp.Tool{
		Name:        "get_capacity_plan",
		Description: "Read a capacity plan by id: its status (DRAFT or PUBLISHED), path capacity, capacity over window, shortage, bottleneck step, the constraint type binding it (bottleneck_constraint) and any composition warnings. Read-only.",
		Annotations: readOnly,
	}, d.getCapacityPlan)

	d.registerReadModelTools(server, idempotent, readOnly)
	d.registerInboundObservedTools(server, idempotent, readOnly)
}

// registerReadModelTools adds the station-capacity tools (ADR 0002) and calls
// registerDemandTools (ADR 0004). Split from registerTools only to keep each
// function within the length budget as the surface grows.
func (d Deps) registerReadModelTools(server *mcp.Server, idempotent func(bool) *mcp.ToolAnnotations, readOnly *mcp.ToolAnnotations) {
	addTool(server, &mcp.Tool{
		Name: "declare_station_standard",
		Description: "Declare the throughput of ONE station of a process at a site (e.g. 180 PACKAGE per 3600 s for PACK at SIM1). Station counts are tallied from facility-layout and carry no throughput of their own; " +
			"count x this standard is composed with the step's other constraints (e.g. LABOR) when a path capacity or capacity plan is computed, and STATION can then be the binding constraint. " +
			"Writes the standard; declaring an existing location/process_type replaces it.",
		Annotations: idempotent(true),
	}, d.declareStationStandard)

	addTool(server, &mcp.Tool{
		Name:        "list_station_standards",
		Description: "List the declared station standards (throughput of one station per location and process type), optionally filtered by location. Read-only.",
		Annotations: readOnly,
	}, d.listStationStandards)

	addTool(server, &mcp.Tool{
		Name: "get_storage_capacity",
		Description: "Read the facility-layout read model of a site: storage positions per zone and location type, and work-center station counts per zone and activity. " +
			"Positions are a count, not a throughput, and no consumed figure exists (stock is never read from inventory-storage). Empty lists when nothing is tallied. Read-only.",
		Annotations: readOnly,
	}, d.getStorageCapacity)

	d.registerDemandTools(server, readOnly)
}

// registerDemandTools adds the expected-demand read tool (docs/adr/0004). It
// is its own method only to keep registerTools within the function-length
// budget as the surface grows.
func (d Deps) registerDemandTools(server *mcp.Server, readOnly *mcp.ToolAnnotations) {
	addTool(server, &mcp.Tool{
		Name: "get_expected_demand",
		Description: "Read the expected demand of a site over a window from the local read model fed by order-management's published order events (OrderAllocated, OrderPartiallyAllocated): " +
			"orders = distinct orders whose promise cutoff falls in [window_start, window_end) (start inclusive, end exclusive), released_lines = lines their latest allocation pass released (NOT units: the events carry no quantities), " +
			"source, and as_of = the time of the newest order event the site's model reflects (null when none). order-management orders carry no site, so every order is attributed to the one configured site. " +
			"Cancellations are not published on the integration topic and are not netted. orders 0 means no data, not zero demand. Read-only.",
		Annotations: readOnly,
	}, d.getExpectedDemand)
}

// addTool registers one tool. A handler error is returned to the SDK as the
// handler's error, which the SDK turns into an isError tool result (never a
// transport/protocol failure), carrying the error text.
func addTool[In, Out any](
	server *mcp.Server,
	tool *mcp.Tool,
	handle func(context.Context, In) (Out, error),
) {
	mcp.AddTool(server, tool, func(ctx context.Context, _ *mcp.CallToolRequest, in In) (*mcp.CallToolResult, Out, error) {
		out, err := handle(ctx, in)
		if err != nil {
			var zero Out
			return nil, zero, err
		}
		return nil, out, nil
	})
}
