package mcp_test

import (
	"testing"
	"time"
)

// One order is enough data: the plan's demand is exactly 1, labelled orders.
func TestCreateCapacityPlanTool_OneOrderIsEnoughDemandData(t *testing.T) {
	h, orders := newDemandHarness(t)
	seedMCPOrder(t, orders, "o-1", "FC01", mcpDemandStart.Add(time.Hour), 1, mcpDemandAsOf)
	h.seedSection43(t, "FC01", "tote-path")

	args := planArgs("FC01", "tote-path", 0)
	delete(args, "assigned_demand")
	got := h.ok(t, "create_capacity_plan", args)
	if got["assigned_demand"] != float64(1) || got["demand_source"] != "orders" || got["shortage"] != float64(0) {
		t.Fatalf("plan = demand %v source %v shortage %v", got["assigned_demand"], got["demand_source"], got["shortage"])
	}
}
