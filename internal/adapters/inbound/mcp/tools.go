package mcp

import (
	"context"
	"time"

	"github.com/modelcontextprotocol/go-sdk/mcp"

	"github.com/claudioed/warehouse-planning/internal/application/ports"
	"github.com/claudioed/warehouse-planning/internal/application/usecases"
	"github.com/claudioed/warehouse-planning/internal/domain/capacityplan"
	"github.com/claudioed/warehouse-planning/internal/domain/processcapacity"
	"github.com/claudioed/warehouse-planning/internal/domain/processpath"
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

type registerConstraintInput struct {
	ProcessType    string  `json:"process_type" jsonschema:"the process step the constraint limits, e.g. PICK, REBIN, PACK"`
	Location       string  `json:"location" jsonschema:"the warehouse location the capacity applies to, e.g. FC01"`
	WindowStart    string  `json:"window_start" jsonschema:"inclusive window start, RFC3339 timestamp, e.g. 2026-10-05T08:00:00Z"`
	WindowEnd      string  `json:"window_end" jsonschema:"exclusive window end, RFC3339 timestamp; must be after window_start"`
	ConstraintType string  `json:"constraint_type" jsonschema:"the kind of constraint: LABOR, LOCATION, EQUIPMENT, STATION, CONVEYOR, BUFFER or REPLENISHMENT"`
	Quantity       float64 `json:"quantity" jsonschema:"the constraint's capacity quantity per period; must not be negative"`
	Unit           string  `json:"unit" jsonschema:"the capacity unit: UNIT, LINE, ORDER or PACKAGE; must match the unit of constraints already registered for this process/location/window"`
	PeriodSeconds  float64 `json:"period_seconds" jsonschema:"the period the quantity is measured over, in seconds; must be positive (3600 = per hour)"`
}

type constraintView struct {
	ConstraintType string  `json:"constraint_type"`
	Quantity       float64 `json:"quantity"`
	Unit           string  `json:"unit"`
	PeriodSeconds  float64 `json:"period_seconds"`
}

type effectiveCapacityOutput struct {
	EffectiveRate     float64          `json:"effective_rate"`
	EffectiveUnit     string           `json:"effective_unit"`
	BindingConstraint string           `json:"binding_constraint"`
	Constraints       []constraintView `json:"constraints,omitempty"`
}

func (d Deps) registerProcessCapacityConstraint(ctx context.Context, in registerConstraintInput) (effectiveCapacityOutput, error) {
	windowStart, windowEnd, err := parseWindow(in.WindowStart, in.WindowEnd)
	if err != nil {
		return effectiveCapacityOutput{}, err
	}
	if in.PeriodSeconds <= 0 {
		return effectiveCapacityOutput{}, toolError("non-positive-period", "period_seconds must be positive")
	}

	result, err := d.RegisterProcessCapacityConstraint.Handle(ctx, usecases.RegisterProcessCapacityConstraintCommand{
		ProcessType:    processcapacity.ProcessType(in.ProcessType),
		Location:       in.Location,
		WindowStart:    windowStart,
		WindowEnd:      windowEnd,
		ConstraintType: processcapacity.ConstraintType(in.ConstraintType),
		Quantity:       in.Quantity,
		Unit:           processcapacity.CapacityUnit(in.Unit),
		Period:         time.Duration(in.PeriodSeconds * float64(time.Second)),
	})
	if err != nil {
		return effectiveCapacityOutput{}, mapError(err)
	}
	return effectiveCapacityOutput{
		EffectiveRate:     result.EffectiveRate.Quantity(),
		EffectiveUnit:     string(result.EffectiveRate.Unit()),
		BindingConstraint: string(result.BindingConstraint),
	}, nil
}

// --- get_effective_process_capacity (read) ------------------------------------

type getEffectiveCapacityInput struct {
	ProcessType string `json:"process_type" jsonschema:"the process step to read, e.g. PICK, REBIN, PACK"`
	Location    string `json:"location" jsonschema:"the warehouse location, e.g. FC01"`
	WindowStart string `json:"window_start" jsonschema:"window start, RFC3339 timestamp; must equal the registered window exactly"`
	WindowEnd   string `json:"window_end" jsonschema:"window end, RFC3339 timestamp; must equal the registered window exactly"`
}

func (d Deps) getEffectiveProcessCapacity(ctx context.Context, in getEffectiveCapacityInput) (effectiveCapacityOutput, error) {
	windowStart, windowEnd, err := parseWindow(in.WindowStart, in.WindowEnd)
	if err != nil {
		return effectiveCapacityOutput{}, err
	}

	pc, err := d.ProcessCapacities.FindByProcessLocationWindow(ctx, processcapacity.ProcessType(in.ProcessType), in.Location, windowStart, windowEnd)
	if err != nil {
		return effectiveCapacityOutput{}, mapError(err)
	}
	if pc == nil {
		return effectiveCapacityOutput{}, toolError("process-capacity-not-found", "no ProcessCapacity registered for the given process_type/location/window")
	}

	effective, binding, err := pc.EffectiveRate()
	if err != nil {
		return effectiveCapacityOutput{}, mapError(err)
	}

	entries := pc.Constraints()
	constraints := make([]constraintView, 0, len(entries))
	for _, entry := range entries {
		constraints = append(constraints, constraintView{
			ConstraintType: string(entry.Type),
			Quantity:       entry.Rate.Quantity(),
			Unit:           string(entry.Rate.Unit()),
			PeriodSeconds:  entry.Rate.Period().Seconds(),
		})
	}
	return effectiveCapacityOutput{
		EffectiveRate:     effective.Quantity(),
		EffectiveUnit:     string(effective.Unit()),
		BindingConstraint: string(binding),
		Constraints:       constraints,
	}, nil
}

// --- register_process_path (write) --------------------------------------------

type registerProcessPathInput struct {
	ID    string   `json:"id" jsonschema:"the process path's id, e.g. tote-path; registering an existing id replaces its name and steps"`
	Name  string   `json:"name" jsonschema:"a human-readable name"`
	Steps []string `json:"steps" jsonschema:"the ordered, non-empty list of step process types, e.g. [PICK, REBIN, PACK]"`
}

type processPathOutput struct {
	ID    string   `json:"id"`
	Name  string   `json:"name"`
	Steps []string `json:"steps"`
}

func (d Deps) registerProcessPath(ctx context.Context, in registerProcessPathInput) (processPathOutput, error) {
	steps := make([]processpath.ProcessType, 0, len(in.Steps))
	for _, step := range in.Steps {
		steps = append(steps, processpath.ProcessType(step))
	}
	path, err := d.RegisterProcessPath.Handle(ctx, usecases.RegisterProcessPathCommand{ID: in.ID, Name: in.Name, Steps: steps})
	if err != nil {
		return processPathOutput{}, mapError(err)
	}
	out := make([]string, 0, len(path.Steps()))
	for _, step := range path.Steps() {
		out = append(out, string(step))
	}
	return processPathOutput{ID: path.ID(), Name: path.Name(), Steps: out}, nil
}

// --- get_process_path_capacity (read) -----------------------------------------

type getPathCapacityInput struct {
	ID               string   `json:"id" jsonschema:"the registered process path's id"`
	Location         string   `json:"location" jsonschema:"the warehouse location every step's capacity is looked up under, e.g. FC01"`
	WindowStart      string   `json:"window_start" jsonschema:"window start, RFC3339 timestamp; every step must have capacity registered for exactly this window"`
	WindowEnd        string   `json:"window_end" jsonschema:"window end, RFC3339 timestamp"`
	UnitsPerOrder    *float64 `json:"units_per_order,omitempty" jsonschema:"workload conversion factor: UNIT per ORDER; required if any step is measured in UNIT"`
	PackagesPerOrder *float64 `json:"packages_per_order,omitempty" jsonschema:"workload conversion factor: PACKAGE per ORDER; required if any step is measured in PACKAGE"`
}

type pathCapacityOutput struct {
	NormalizedRate float64 `json:"normalized_rate"`
	NormalizedUnit string  `json:"normalized_unit"`
	BottleneckStep string  `json:"bottleneck_step"`
}

func (d Deps) getProcessPathCapacity(ctx context.Context, in getPathCapacityInput) (pathCapacityOutput, error) {
	windowStart, windowEnd, err := parseWindow(in.WindowStart, in.WindowEnd)
	if err != nil {
		return pathCapacityOutput{}, err
	}
	result, err := d.GetProcessPathCapacity.Handle(ctx, usecases.GetProcessPathCapacityCommand{
		ProcessPathID:    in.ID,
		Location:         in.Location,
		WindowStart:      windowStart,
		WindowEnd:        windowEnd,
		UnitsPerOrder:    in.UnitsPerOrder,
		PackagesPerOrder: in.PackagesPerOrder,
	})
	if err != nil {
		return pathCapacityOutput{}, mapError(err)
	}
	return pathCapacityOutput{
		NormalizedRate: result.NormalizedRate.Quantity(),
		NormalizedUnit: string(result.NormalizedRate.Unit()),
		BottleneckStep: string(result.BottleneckStep),
	}, nil
}

// --- create_capacity_plan (write) ---------------------------------------------

type createPlanInput struct {
	WarehouseID      string   `json:"warehouse_id" jsonschema:"the warehouse the plan is for, e.g. WH-1"`
	Location         string   `json:"location" jsonschema:"the location whose step capacities are used, e.g. FC01"`
	WindowStart      string   `json:"window_start" jsonschema:"plan window start, RFC3339 timestamp; every step needs capacity registered for exactly this window"`
	WindowEnd        string   `json:"window_end" jsonschema:"plan window end, RFC3339 timestamp; must be after window_start"`
	PathID           string   `json:"path_id" jsonschema:"the registered process path to evaluate"`
	AssignedDemand   *float64 `json:"assigned_demand" jsonschema:"the demand assigned to the window, in ORDERS; required (never silently zero) and must not be negative"`
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
	ID                 string  `json:"id"`
	WarehouseID        string  `json:"warehouse_id"`
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
}

func toCapacityPlanOutput(p *capacityplan.CapacityPlan) capacityPlanOutput {
	out := capacityPlanOutput{
		ID:                 p.ID(),
		WarehouseID:        p.WarehouseID(),
		Location:           p.Location(),
		WindowStart:        p.Window().Start().UTC().Format(time.RFC3339),
		WindowEnd:          p.Window().End().UTC().Format(time.RFC3339),
		PathID:             p.ProcessPathID(),
		AssignedDemand:     p.AssignedDemand(),
		Status:             string(p.Status()),
		PathCapacity:       p.PathCapacity(),
		BottleneckStep:     string(p.BottleneckStep()),
		CapacityOverWindow: p.CapacityOverWindow(),
		Shortage:           p.Shortage(),
		CreatedAt:          p.CreatedAt().UTC().Format(time.RFC3339),
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
	if in.AssignedDemand == nil {
		return capacityPlanOutput{}, toolError("missing-assigned-demand", "assigned_demand (orders) must be provided")
	}

	// The use case saves the plan and inserts its CloudEvents into the
	// transactional outbox in one UnitOfWork; the relay in cmd/api drains it.
	plan, err := d.CreateCapacityPlan.Handle(ctx, usecases.CreateCapacityPlanCommand{
		WarehouseID:      in.WarehouseID,
		Location:         in.Location,
		WindowStart:      windowStart,
		WindowEnd:        windowEnd,
		ProcessPathID:    in.PathID,
		AssignedDemand:   *in.AssignedDemand,
		UnitsPerOrder:    in.UnitsPerOrder,
		PackagesPerOrder: in.PackagesPerOrder,
	})
	if err != nil {
		return capacityPlanOutput{}, mapError(err)
	}
	return toCapacityPlanOutput(plan), nil
}

// --- publish_capacity_plan (write) --------------------------------------------

func (d Deps) publishCapacityPlan(ctx context.Context, in planIDInput) (capacityPlanOutput, error) {
	plan, err := d.PublishCapacityPlan.Handle(ctx, in.ID)
	if err != nil {
		return capacityPlanOutput{}, mapError(err)
	}
	return toCapacityPlanOutput(plan), nil
}

// --- get_capacity_plan (read) -------------------------------------------------

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
		Description: "Compute a registered process path's end-to-end capacity at a location and exact window, normalized to ORDER per hour, and the bottleneck step. " +
			"Every step needs a registered capacity; units_per_order / packages_per_order convert UNIT and PACKAGE steps. Read-only.",
		Annotations: readOnly,
	}, d.getProcessPathCapacity)

	addTool(server, &mcp.Tool{
		Name: "create_capacity_plan",
		Description: "Evaluate assigned demand (orders) against a process path's capacity over a window and store a DRAFT capacity plan, returning " +
			"path capacity (ORDER/hour), capacity over the window, shortage (orders, 0 if none) and the bottleneck step. " +
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
		Description: "Read a capacity plan by id: its status (DRAFT or PUBLISHED), path capacity, capacity over window, shortage and bottleneck step. Read-only.",
		Annotations: readOnly,
	}, d.getCapacityPlan)
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
