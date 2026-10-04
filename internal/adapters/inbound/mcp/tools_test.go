package mcp_test

import (
	"os"
	"reflect"
	"regexp"
	"strconv"
	"testing"
)

// featureExpectation reads the numbers features/capacity_plan.feature's
// section-43 scenario asserts through REST ("the capacity plan is DRAFT with
// path capacity 1000 ORDER per HOUR, capacity over window 8000, shortage 4000
// and bottleneck REBIN"), so the MCP surface is pinned to the SAME values
// rather than to numbers retyped here.
type featureExpectation struct {
	pathCapacity, overWindow, shortage float64
	bottleneck                         string
}

var featureLine = regexp.MustCompile(`the capacity plan is DRAFT with path capacity (\d+(?:\.\d+)?) ORDER per HOUR, capacity over window (\d+(?:\.\d+)?), shortage (\d+(?:\.\d+)?) and bottleneck ([A-Z-]+)`)

func section43FromFeature(t *testing.T) featureExpectation {
	t.Helper()
	raw, err := os.ReadFile("../../../../features/capacity_plan.feature")
	if err != nil {
		t.Fatalf("read feature: %v", err)
	}
	m := featureLine.FindSubmatch(raw) // the FIRST DRAFT assertion is the section-43 scenario
	if m == nil {
		t.Fatal("capacity_plan.feature no longer asserts a DRAFT plan line; update this test")
	}
	f := func(b []byte) float64 {
		v, err := strconv.ParseFloat(string(b), 64)
		if err != nil {
			t.Fatal(err)
		}
		return v
	}
	return featureExpectation{pathCapacity: f(m[1]), overWindow: f(m[2]), shortage: f(m[3]), bottleneck: string(m[4])}
}

// The design doc's section-43 worked example, end to end through MCP tools
// only: 12000 orders over 8h on PICK -> REBIN -> PACK is short by 4000,
// bound by REBIN; publish once, a second publish is a tool error.
func TestSection43ThroughMCPTools(t *testing.T) {
	want := section43FromFeature(t)
	if want.shortage != 4000 || want.bottleneck != "REBIN" {
		t.Fatalf("feature expectation drifted from the section-43 numbers: %+v", want)
	}
	h := newHarness(t)

	registerAndCheckPath(t, h, want)
	id := createAndCheckDraft(t, h, want)
	publishTwice(t, h, id, want)
	checkGetPlan(t, h, id, want)
}

func registerAndCheckPath(t *testing.T, h *harness, want featureExpectation) {
	t.Helper()
	// Registering capacities returns the running effective rate + binding constraint.
	first := h.ok(t, "register_process_capacity_constraint", constraintArgs("PICK", "FC01", 4000, "UNIT"))
	if first["effective_rate"] != 4000.0 || first["effective_unit"] != "UNIT" || first["binding_constraint"] != "LABOR" {
		t.Fatalf("PICK registration = %v", first)
	}
	h.ok(t, "register_process_capacity_constraint", constraintArgs("REBIN", "FC01", 2500, "UNIT"))
	h.ok(t, "register_process_capacity_constraint", constraintArgs("PACK", "FC01", 1800, "PACKAGE"))

	path := h.ok(t, "register_process_path", map[string]any{"id": "tote-path", "name": "Tote path", "steps": []string{"PICK", "REBIN", "PACK"}})
	if path["id"] != "tote-path" || !reflect.DeepEqual(path["steps"], []any{"PICK", "REBIN", "PACK"}) {
		t.Fatalf("path = %v", path)
	}

	pathCap := h.ok(t, "get_process_path_capacity", map[string]any{
		"id": "tote-path", "location": "FC01", "window_start": winStart, "window_end": winEnd,
		"units_per_order": 2.5, "packages_per_order": 1,
	})
	if pathCap["normalized_rate"] != want.pathCapacity || pathCap["normalized_unit"] != "ORDER" || pathCap["bottleneck_step"] != want.bottleneck {
		t.Fatalf("path capacity = %v, want %+v", pathCap, want)
	}
}

func checkGetPlan(t *testing.T, h *harness, id string, want featureExpectation) {
	t.Helper()
	got := h.ok(t, "get_capacity_plan", map[string]any{"id": id})
	assertPlan(t, got, "PUBLISHED", want)
	if got["created_at"] == nil || got["warehouse_id"] != "WH-1" || got["location"] != "FC01" || got["path_id"] != "tote-path" ||
		got["assigned_demand"] != 12000.0 || got["window_start"] != winStart || got["window_end"] != winEnd {
		t.Fatalf("get_capacity_plan body = %v", got)
	}
}

func createAndCheckDraft(t *testing.T, h *harness, want featureExpectation) string {
	t.Helper()
	plan := h.ok(t, "create_capacity_plan", planArgs("FC01", "tote-path", 12000))
	assertPlan(t, plan, "DRAFT", want)
	if _, published := plan["published_at"]; published {
		t.Fatalf("a DRAFT plan must have no published_at: %v", plan)
	}
	id, _ := plan["id"].(string)
	if id == "" {
		t.Fatalf("plan has no id: %v", plan)
	}
	if got := eventTypes(h.outbox); !reflect.DeepEqual(got, []string{"CapacityPlanCreated"}) {
		t.Fatalf("outbox after create = %v", got)
	}
	return id
}

func publishTwice(t *testing.T, h *harness, id string, want featureExpectation) {
	t.Helper()
	published := h.ok(t, "publish_capacity_plan", map[string]any{"id": id})
	assertPlan(t, published, "PUBLISHED", want)
	if published["id"] != id || published["published_at"] == nil {
		t.Fatalf("published plan = %v", published)
	}
	wantEvents := []string{"CapacityPlanCreated", "CapacityPlanPublished", "CapacityShortageDetected", "BottleneckDetected"}
	if got := eventTypes(h.outbox); !reflect.DeepEqual(got, wantEvents) {
		t.Fatalf("outbox after publish = %v, want %v", got, wantEvents)
	}

	// Publishing again is a TOOL error (isError), not a transport failure, and queues nothing.
	h.fail(t, "publish_capacity_plan", map[string]any{"id": id}, "capacity-plan-already-published")
	if got := eventTypes(h.outbox); !reflect.DeepEqual(got, wantEvents) {
		t.Fatalf("a rejected publish queued events: %v", got)
	}
}

func assertPlan(t *testing.T, plan map[string]any, status string, want featureExpectation) {
	t.Helper()
	if plan["status"] != status || plan["path_capacity"] != want.pathCapacity || plan["capacity_over_window"] != want.overWindow ||
		plan["shortage"] != want.shortage || plan["bottleneck_step"] != want.bottleneck {
		t.Fatalf("plan = %v, want status %s and %+v", plan, status, want)
	}
}

// Within capacity (feature scenario 2): no shortage, only Created+Published queued.
func TestWithinCapacityHasNoShortage(t *testing.T) {
	h := newHarness(t)
	h.seedSection43(t, "FC02", "tote-path")
	plan := h.ok(t, "create_capacity_plan", planArgs("FC02", "tote-path", 6000))
	if plan["shortage"] != 0.0 || plan["status"] != "DRAFT" || plan["bottleneck_step"] != "REBIN" {
		t.Fatalf("plan = %v", plan)
	}
	h.ok(t, "publish_capacity_plan", map[string]any{"id": plan["id"]})
	if got := eventTypes(h.outbox); !reflect.DeepEqual(got, []string{"CapacityPlanCreated", "CapacityPlanPublished"}) {
		t.Fatalf("outbox = %v", got)
	}
}

func TestGetEffectiveProcessCapacity(t *testing.T) {
	h := newHarness(t)
	args := constraintArgs("PICK", "FC01", 4000, "UNIT")
	h.ok(t, "register_process_capacity_constraint", args)
	// A second, tighter constraint of another type becomes the binding one.
	args["constraint_type"], args["quantity"] = "EQUIPMENT", 3000.0
	reg := h.ok(t, "register_process_capacity_constraint", args)
	if reg["effective_rate"] != 3000.0 || reg["binding_constraint"] != "EQUIPMENT" {
		t.Fatalf("registration = %v", reg)
	}

	got := h.ok(t, "get_effective_process_capacity", map[string]any{
		"process_type": "PICK", "location": "FC01", "window_start": winStart, "window_end": winEnd,
	})
	if got["effective_rate"] != 3000.0 || got["effective_unit"] != "UNIT" || got["binding_constraint"] != "EQUIPMENT" {
		t.Fatalf("effective capacity = %v", got)
	}
	constraints, _ := got["constraints"].([]any)
	if len(constraints) != 2 {
		t.Fatalf("constraints = %v, want 2 entries", got["constraints"])
	}
	for _, c := range constraints {
		entry := c.(map[string]any)
		if entry["unit"] != "UNIT" || entry["period_seconds"] != 3600.0 {
			t.Fatalf("constraint entry = %v", entry)
		}
	}
}

// Domain and validation failures must come back as isError tool results
// carrying the REST error slug -- never as protocol errors.
func TestErrorsSurfaceAsToolErrors(t *testing.T) {
	h := newHarness(t)
	h.seedSection43(t, "FC01", "tote-path")
	h.ok(t, "register_process_path", map[string]any{"id": "pick-only", "name": "Pick only", "steps": []string{"PICK"}})

	win := func(extra map[string]any) map[string]any {
		m := map[string]any{"process_type": "PICK", "location": "FC01", "window_start": winStart, "window_end": winEnd}
		for k, v := range extra {
			m[k] = v
		}
		return m
	}
	pathWin := func(id string) map[string]any {
		return map[string]any{"id": id, "location": "FC01", "window_start": winStart, "window_end": winEnd, "units_per_order": 2.5, "packages_per_order": 1}
	}

	tests := []struct {
		name string
		tool string
		args map[string]any
		want string
	}{
		{"malformed window_start", "register_process_capacity_constraint", func() map[string]any {
			a := constraintArgs("PICK", "FC01", 1, "UNIT")
			a["window_start"] = "yesterday"
			return a
		}(), "malformed-window-start"},
		{"malformed window_end", "get_effective_process_capacity", win(map[string]any{"window_end": "later"}), "malformed-window-end"},
		{"end not after start", "register_process_capacity_constraint", func() map[string]any {
			a := constraintArgs("PICK", "FC01", 1, "UNIT")
			a["window_end"] = winStart
			return a
		}(), "invalid-capacity-window"},
		{"zero period", "register_process_capacity_constraint", func() map[string]any {
			a := constraintArgs("PICK", "FC01", 1, "UNIT")
			a["period_seconds"] = 0
			return a
		}(), "non-positive-period"},
		{"negative quantity", "register_process_capacity_constraint", constraintArgs("PICK", "FC01", -1, "UNIT"), "negative-quantity"},
		{"unit mismatch", "register_process_capacity_constraint", constraintArgs("PICK", "FC01", 1, "PACKAGE"), "unit-mismatch"},
		{"nothing registered", "get_effective_process_capacity", win(map[string]any{"location": "NOWHERE"}), "process-capacity-not-found"},
		{"empty steps", "register_process_path", map[string]any{"id": "x", "name": "x", "steps": []string{}}, "empty-process-path-steps"},
		{"unknown path capacity", "get_process_path_capacity", pathWin("nowhere-path"), "process-path-not-found"},
		{"missing step capacity", "get_process_path_capacity", func() map[string]any {
			m := pathWin("tote-path")
			m["location"] = "FC99"
			return m
		}(), "missing-step-capacity"},
		{"missing conversion factor", "get_process_path_capacity", func() map[string]any {
			m := pathWin("tote-path")
			delete(m, "units_per_order")
			return m
		}(), "missing-conversion-factor"},
		{"non-positive conversion factor", "get_process_path_capacity", func() map[string]any {
			m := pathWin("tote-path")
			m["units_per_order"] = 0
			return m
		}(), "non-positive-conversion-factor"},
		{"plan for unknown path", "create_capacity_plan", planArgs("FC01", "nowhere-path", 100), "process-path-not-found"},
		{"negative demand", "create_capacity_plan", planArgs("FC01", "pick-only", -5), "negative-assigned-demand"},
		{"null demand", "create_capacity_plan", func() map[string]any {
			m := planArgs("FC01", "pick-only", 0)
			m["assigned_demand"] = nil
			return m
		}(), "missing-assigned-demand"},
		{"blank warehouse", "create_capacity_plan", func() map[string]any {
			m := planArgs("FC01", "pick-only", 1)
			m["warehouse_id"] = ""
			return m
		}(), "missing-required-field"},
		{"malformed plan window", "create_capacity_plan", func() map[string]any {
			m := planArgs("FC01", "pick-only", 1)
			m["window_end"] = "nope"
			return m
		}(), "malformed-window-end"},
		{"publish unknown plan", "publish_capacity_plan", map[string]any{"id": "no-such-plan"}, "capacity-plan-not-found"},
		{"get unknown plan", "get_capacity_plan", map[string]any{"id": "no-such-plan"}, "capacity-plan-not-found"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) { h.fail(t, tc.tool, tc.args, tc.want) })
	}
	if len(h.outbox.Messages()) != 0 {
		t.Fatalf("rejected calls queued outbox events: %v", eventTypes(h.outbox))
	}
}

// A missing REQUIRED argument is rejected by the tool's input schema; that
// must still reach the client as a result or a clean error, never hang or
// create a plan. assigned_demand in particular must never silently be zero.
func TestMissingRequiredDemandIsRejected(t *testing.T) {
	h := newHarness(t)
	h.seedSection43(t, "FC01", "tote-path")
	args := planArgs("FC01", "tote-path", 0)
	delete(args, "assigned_demand")
	res, err := h.session.CallTool(t.Context(), callParams("create_capacity_plan", args))
	if err == nil && !res.IsError {
		t.Fatalf("a plan was created without assigned_demand: %s", asJSON(t, res.StructuredContent))
	}
	if len(h.outbox.Messages()) != 0 {
		t.Fatalf("outbox = %v", eventTypes(h.outbox))
	}
}

// An unexpected (non-domain) repository failure is reported generically:
// no infrastructure detail reaches the model.
func TestUnexpectedErrorsAreGeneric(t *testing.T) {
	deps, _ := newDeps()
	deps.CapacityPlans = failingPlans{}
	h := &harness{session: connectSession(t, deps)}
	h.fail(t, "get_capacity_plan", map[string]any{"id": "p1"}, "internal-error")
	res := h.call(t, "get_capacity_plan", map[string]any{"id": "p1"})
	if got := text(res); regexp.MustCompile(`postgres|dsn|secret`).MatchString(got) {
		t.Fatalf("infrastructure detail leaked to the model: %q", got)
	}
}
