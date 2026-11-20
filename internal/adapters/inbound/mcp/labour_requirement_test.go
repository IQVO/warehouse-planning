package mcp_test

import (
	"context"
	"slices"
	"testing"
	"time"

	"github.com/claudioed/warehouse-planning/internal/adapters/outbound/memory"
	"github.com/claudioed/warehouse-planning/internal/domain/laborstandard"
)

// newLabourHarness is the standard stack with the labour standards copy
// wired (docs/adr/0022), exactly as cmd/mcp wires it: PACK 90 s for every
// site, PICK 12 s scoped to SIM1.
func newLabourHarness(t *testing.T) *harness {
	t.Helper()
	deps, outboxRepo, tallyRepo, paths := newStack()
	standards := memory.NewLaborStandardRepo()
	from := time.Date(2026, 10, 1, 0, 0, 0, 0, time.UTC)
	for _, p := range []laborstandard.Params{
		{StandardID: "std-pack", TaskType: "PACK", ExpectedSeconds: 90, EffectiveFrom: from, AsOf: from},
		{StandardID: "std-pick", TaskType: "PICK", SiteCode: "SIM1", ExpectedSeconds: 12, EffectiveFrom: from, AsOf: from},
	} {
		s, err := laborstandard.New(p)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := standards.Record(context.Background(), s); err != nil {
			t.Fatal(err)
		}
	}
	deps.CreateCapacityPlan.LaborStandards = standards
	return &harness{session: connectSession(t, deps), outbox: outboxRepo, deps: deps, tally: tallyRepo, paths: paths}
}

func labourItems(t *testing.T, plan map[string]any) []map[string]any {
	t.Helper()
	raw, present := plan["labour_requirement"]
	items, isList := raw.([]any)
	if !present || !isList {
		t.Fatalf("labour_requirement = %#v, want a list (never null)", raw)
	}
	out := make([]map[string]any, 0, len(items))
	for _, it := range items {
		m, _ := it.(map[string]any)
		out = append(out, m)
	}
	return out
}

func TestCreateCapacityPlanTool_StatesTheLabourEachStepRequires(t *testing.T) {
	h := newLabourHarness(t)
	h.seedSection43(t, "FC01", "tote-path")
	args := planArgs("FC01", "tote-path", 12000)
	args["tasks_per_order"] = map[string]any{"PICK": 3, "PACK": 1}
	created := h.ok(t, "create_capacity_plan", args)
	if created["shortage"] != float64(4000) || created["bottleneck_step"] != "REBIN" {
		t.Fatalf("the requirement changed the plan: shortage %v bottleneck %v", created["shortage"], created["bottleneck_step"])
	}
	id, _ := created["id"].(string)
	stored := h.ok(t, "get_capacity_plan", map[string]any{"id": id})
	assertPricedLabour(t, "create_capacity_plan", created)
	assertPricedLabour(t, "get_capacity_plan", stored)
}

// assertPricedLabour checks the 12000-order plan priced with PICK 3 and
// PACK 1 tasks per order.
func assertPricedLabour(t *testing.T, name string, plan map[string]any) {
	t.Helper()
	items := labourItems(t, plan)
	if len(items) != 3 {
		t.Fatalf("%s: %d items, want one per step", name, len(items))
	}
	want := []map[string]any{
		{"process_type": "PICK", "known": true, "tasks": float64(36000), "required_labour_hours": float64(120), "required_average_heads": float64(15), "standard_site_scoped": true},
		{"process_type": "REBIN", "known": false, "reason": "no-labor-standard"},
		{"process_type": "PACK", "known": true, "standard_seconds": float64(90), "required_labour_hours": float64(300)},
	}
	for i, fields := range want {
		for k, v := range fields {
			if items[i][k] != v {
				t.Errorf("%s step %d %s = %v, want %v", name, i, k, items[i][k], v)
			}
		}
	}
}

func TestCreateCapacityPlanTool_WithoutAFactorTheStepIsUnknown(t *testing.T) {
	h := newLabourHarness(t)
	h.seedSection43(t, "FC01", "tote-path")
	created := h.ok(t, "create_capacity_plan", planArgs("FC01", "tote-path", 12000))
	for _, item := range labourItems(t, created) {
		want := "no-task-factor"
		if item["process_type"] == "REBIN" {
			want = "no-labor-standard"
		}
		if item["known"] != false || item["reason"] != want || item["tasks"] != float64(0) {
			t.Errorf("%v = %v, want known false / %s", item["process_type"], item, want)
		}
	}
}

func TestCreateCapacityPlanTool_NonPositiveTaskFactorIsRejected(t *testing.T) {
	h := newLabourHarness(t)
	h.seedSection43(t, "FC01", "tote-path")
	args := planArgs("FC01", "tote-path", 12000)
	args["tasks_per_order"] = map[string]any{"PACK": 0}
	h.fail(t, "create_capacity_plan", args, "non-positive-tasks-per-order")
}

func TestCreateCapacityPlanTool_TasksPerOrderIsOptionalInTheSchema(t *testing.T) {
	h := newLabourHarness(t)
	res, err := h.session.ListTools(t.Context(), nil)
	if err != nil {
		t.Fatal(err)
	}
	for _, tool := range res.Tools {
		if tool.Name != "create_capacity_plan" {
			continue
		}
		schema, _ := tool.InputSchema.(map[string]any)
		required, _ := schema["required"].([]any)
		if slices.Contains(required, any("tasks_per_order")) {
			t.Fatalf("tasks_per_order must be optional, required = %v", required)
		}
		props, _ := schema["properties"].(map[string]any)
		if _, ok := props["tasks_per_order"]; !ok {
			t.Fatal("tasks_per_order is not a documented argument")
		}
		return
	}
	t.Fatal("create_capacity_plan not advertised")
}
