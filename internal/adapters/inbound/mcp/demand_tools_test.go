package mcp_test

import (
	"context"
	"fmt"
	"slices"
	"testing"
	"time"

	"github.com/claudioed/warehouse-planning/internal/adapters/outbound/memory"
	"github.com/claudioed/warehouse-planning/internal/application/usecases"
	"github.com/claudioed/warehouse-planning/internal/domain/demand"
)

var (
	mcpDemandStart = time.Date(2026, 10, 5, 8, 0, 0, 0, time.UTC)
	mcpDemandEnd   = time.Date(2026, 10, 5, 16, 0, 0, 0, time.UTC)
	mcpDemandAsOf  = time.Date(2026, 10, 4, 9, 15, 30, 0, time.UTC)
)

// newDemandHarness is the standard in-memory stack with the expected-demand
// read model wired in, exactly as cmd/mcp wires it.
func newDemandHarness(t *testing.T) (*harness, *memory.OrderDemandRepo) {
	t.Helper()
	deps, outboxRepo, tallyRepo := newStack()
	orders := memory.NewOrderDemandRepo()
	expected := &usecases.GetExpectedDemand{Demand: orders}
	deps.GetExpectedDemand = expected
	deps.CreateCapacityPlan.Demand = expected
	return &harness{session: connectSession(t, deps), outbox: outboxRepo, deps: deps, tally: tallyRepo}, orders
}

func seedMCPOrder(t *testing.T, repo *memory.OrderDemandRepo, id, location string, promise time.Time, lines int, asOf time.Time) {
	t.Helper()
	o, err := demand.NewOrder(demand.OrderParams{OrderID: id, Location: location, PromiseAt: promise, ReleasedLines: lines, AsOf: asOf})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := repo.Upsert(context.Background(), o); err != nil {
		t.Fatal(err)
	}
}

func demandArgs(location, start, end string) map[string]any {
	return map[string]any{"location": location, "window_start": start, "window_end": end}
}

func TestGetExpectedDemandTool_CountsTheHalfOpenWindow(t *testing.T) {
	h, orders := newDemandHarness(t)
	seedMCPOrder(t, orders, "at-start", "SIM1", mcpDemandStart, 2, mcpDemandAsOf)
	seedMCPOrder(t, orders, "last-second", "SIM1", mcpDemandEnd.Add(-time.Second), 3, mcpDemandAsOf.Add(time.Minute))
	seedMCPOrder(t, orders, "at-end", "SIM1", mcpDemandEnd, 5, mcpDemandAsOf.Add(time.Hour))
	seedMCPOrder(t, orders, "other-site", "SIM2", mcpDemandStart.Add(time.Hour), 7, mcpDemandAsOf.Add(2*time.Hour))

	got := h.ok(t, "get_expected_demand", demandArgs("SIM1", winStart, winEnd))
	want := map[string]any{
		"location": "SIM1", "window_start": winStart, "window_end": winEnd,
		"orders": float64(2), "released_lines": float64(5), "source": "order-management",
		"as_of": "2026-10-04T10:15:30Z",
	}
	for k, v := range want {
		if got[k] != v {
			t.Errorf("%s = %v, want %v", k, got[k], v)
		}
	}
}

func TestGetExpectedDemandTool_NoDataIsZeroOrdersAndNullAsOf(t *testing.T) {
	h, _ := newDemandHarness(t)
	got := h.ok(t, "get_expected_demand", demandArgs("SIM1", winStart, winEnd))
	asOf, present := got["as_of"]
	if !present || asOf != nil || got["orders"] != float64(0) {
		t.Fatalf("result = %v", got)
	}
}

func TestGetExpectedDemandTool_Rejections(t *testing.T) {
	h, _ := newDemandHarness(t)
	for _, tc := range []struct {
		name string
		args map[string]any
		want string
	}{
		{"blank location", demandArgs("", winStart, winEnd), "missing-location"},
		{"malformed start", demandArgs("SIM1", "yesterday", winEnd), "malformed-window-start"},
		{"malformed end", demandArgs("SIM1", winStart, "soon"), "malformed-window-end"},
		{"empty window", demandArgs("SIM1", winStart, winStart), "invalid-capacity-window"},
		{"inverted window", demandArgs("SIM1", winEnd, winStart), "invalid-capacity-window"},
	} {
		t.Run(tc.name, func(t *testing.T) { h.fail(t, "get_expected_demand", tc.args, tc.want) })
	}
}

func TestCreateCapacityPlanTool_AssignedDemandIsOptionalInTheSchema(t *testing.T) {
	h, _ := newDemandHarness(t)
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
		if slices.Contains(required, any("assigned_demand")) {
			t.Fatalf("assigned_demand must be optional, required = %v", required)
		}
		props, _ := schema["properties"].(map[string]any)
		if _, ok := props["assigned_demand"]; !ok {
			t.Fatal("assigned_demand is no longer a documented argument")
		}
		return
	}
	t.Fatal("create_capacity_plan not advertised")
}

func TestCreateCapacityPlanTool_OmittedDemandUsesOrdersAndSaysSo(t *testing.T) {
	h, orders := newDemandHarness(t)
	h.seedSection43(t, "FC01", "tote-path")
	for i := 0; i < 8500; i++ { // capacity over the 8h window is 8000
		seedMCPOrder(t, orders, fmt.Sprintf("o-%d", i), "FC01", mcpDemandStart.Add(time.Duration(i%7)*time.Hour), 1, mcpDemandAsOf)
	}
	seedMCPOrder(t, orders, "at-end", "FC01", mcpDemandEnd, 1, mcpDemandAsOf)

	args := planArgs("FC01", "tote-path", 0)
	delete(args, "assigned_demand")
	got := h.ok(t, "create_capacity_plan", args)
	if got["assigned_demand"] != float64(8500) || got["demand_source"] != "orders" || got["shortage"] != float64(500) {
		t.Fatalf("plan = demand %v source %v shortage %v", got["assigned_demand"], got["demand_source"], got["shortage"])
	}
	id, _ := got["id"].(string)
	if stored := h.ok(t, "get_capacity_plan", map[string]any{"id": id}); stored["demand_source"] != "orders" {
		t.Fatalf("get_capacity_plan lost the source: %v", stored["demand_source"])
	}
}

func TestCreateCapacityPlanTool_ExplicitDemandWinsAndOmittedWithoutDataFails(t *testing.T) {
	h, orders := newDemandHarness(t)
	h.seedSection43(t, "FC01", "tote-path")
	seedMCPOrder(t, orders, "o-1", "FC01", mcpDemandStart.Add(time.Hour), 1, mcpDemandAsOf)

	got := h.ok(t, "create_capacity_plan", planArgs("FC01", "tote-path", 12000))
	if got["assigned_demand"] != float64(12000) || got["demand_source"] != "request" {
		t.Fatalf("plan = demand %v source %v", got["assigned_demand"], got["demand_source"])
	}

	// No orders at THIS location: the model has data elsewhere, which must not count.
	args := planArgs("FC02", "tote-path", 0)
	delete(args, "assigned_demand")
	h.fail(t, "create_capacity_plan", args, "missing-assigned-demand")
}
