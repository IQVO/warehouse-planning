package mcp_test

import (
	"context"
	"testing"
	"time"

	"github.com/claudioed/warehouse-planning/internal/adapters/outbound/memory"
	"github.com/claudioed/warehouse-planning/internal/domain/workloadforecast"
)

// create_capacity_plan takes demand_source forecast and forecast_path_id
// (docs/adr/0024 decision 1) as inputs of the SAME tool: no new tool.
func TestCreateCapacityPlan_FromTheForecast(t *testing.T) {
	h := newHarness(t)
	forecasts := memory.NewWorkloadForecastRepo()
	h.deps.CreateCapacityPlan.Forecasts = forecasts
	start, _ := time.Parse(time.RFC3339, winStart)
	f, _, err := workloadforecast.New("SIM1", "pick", []workloadforecast.Bucket{{CPT: start, Quantity: 101}}, start)
	if err != nil {
		t.Fatal(err)
	}
	if err := forecasts.Save(context.Background(), f); err != nil {
		t.Fatal(err)
	}
	h.seedSection43(t, "FC01", "tote")

	args := planArgs("FC01", "tote", 0)
	delete(args, "assigned_demand")
	args["demand_source"], args["forecast_path_id"] = "forecast", "Pick"
	plan := h.ok(t, "create_capacity_plan", args)
	if plan["assigned_demand"] != 41.0 || plan["demand_source"] != "forecast" || plan["forecast_path_id"] != "PICK" {
		t.Fatalf("plan = %v, want ceil(101 / 2.5) = 41 orders from the PICK forecast", plan)
	}

	delete(args, "units_per_order")
	h.fail(t, "create_capacity_plan", args, "missing-units-per-order")
	args["units_per_order"], args["assigned_demand"] = 2.5, 5
	h.fail(t, "create_capacity_plan", args, "demand-source-conflict")
	delete(args, "assigned_demand")
	args["forecast_path_id"] = "PACK"
	h.fail(t, "create_capacity_plan", args, "missing-assigned-demand")
	args["demand_source"] = "inbound"
	h.fail(t, "create_capacity_plan", args, "unknown-demand-source")
}
