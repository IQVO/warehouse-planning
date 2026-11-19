package main_test

import (
	"context"
	"fmt"
	"net/http"
	"strconv"
	"strings"

	"github.com/cucumber/godog"
)

// planRequest is the POST /capacity-plans body of the docs/adr/0024 steps:
// no assigned_demand, packages_per_order 1, plus whatever a step adds.
func planRequest(warehouse, site, location, pathID, start, end string) map[string]any {
	return map[string]any{
		"warehouse_id": warehouse, "site_id": site, "location": location, "path_id": pathID,
		"window_start": start, "window_end": end, "packages_per_order": 1,
	}
}

// createPlan POSTs a plan body and, on 201, remembers its id for the
// publish/get steps.
func (w *world) createPlan(ctx context.Context, body map[string]any) error {
	if err := w.record(ctx, http.MethodPost, "/capacity-plans", body); err != nil {
		return err
	}
	if w.status != http.StatusCreated {
		return nil
	}
	var plan struct {
		ID string `json:"id"`
	}
	if err := w.decode(&plan); err != nil {
		return err
	}
	w.planID = plan.ID
	return nil
}

// iCreateAPlanFromTheForecast creates a demand_source forecast plan. path is
// the forecast_path_id ("" leaves it out: the plan's path_id is read) and
// units the units_per_order ("" leaves it out).
func (w *world) iCreateAPlanFromTheForecast(ctx context.Context, warehouse, site, location, pathID, start, end, forecastPath, units string) error {
	body := planRequest(warehouse, site, location, pathID, start, end)
	body["demand_source"] = "forecast"
	if forecastPath != "" {
		body["forecast_path_id"] = forecastPath
	}
	if units != "" {
		n, err := strconv.ParseFloat(units, 64)
		if err != nil {
			return err
		}
		body["units_per_order"] = n
	}
	return w.createPlan(ctx, body)
}

func (w *world) theCapacityPlanReadTheForecastOfPath(want string) error {
	var plan map[string]any
	if err := w.decode(&plan); err != nil {
		return err
	}
	if plan["forecast_path_id"] != want {
		return fmt.Errorf("forecast_path_id = %v, want %q (%s)", plan["forecast_path_id"], want, string(w.body))
	}
	return nil
}

// iCreateALabelledPlan creates a plan with a stated demand (units_per_order
// 1) and the scenario label (docs/adr/0024 decision 2).
func (w *world) iCreateALabelledPlan(ctx context.Context, warehouse, site, location, pathID, start, end string, demand float64, label string) error {
	body := planRequest(warehouse, site, location, pathID, start, end)
	body["assigned_demand"], body["units_per_order"], body["scenario"] = demand, 1, label
	return w.createPlan(ctx, body)
}

// theCapacityPlanListHoldsTheScenarios checks the scenario labels of a list
// page, in order ("a, b").
func (w *world) theCapacityPlanListHoldsTheScenarios(want string) error {
	var page struct {
		Items []struct {
			Scenario string `json:"scenario"`
		} `json:"items"`
	}
	if err := w.decode(&page); err != nil {
		return err
	}
	got := make([]string, 0, len(page.Items))
	for _, item := range page.Items {
		got = append(got, item.Scenario)
	}
	if strings.Join(got, ", ") != want {
		return fmt.Errorf("listed scenarios %q, want %q", strings.Join(got, ", "), want)
	}
	return nil
}

func registerScenarioSteps(sc *godog.ScenarioContext, w *world) {
	const ts = `"([^"]*)"`
	const plan = `^I create a capacity plan for warehouse ` + ts + ` at site ` + ts + ` in location ` + ts + ` on path ` + ts + ` for the window ` + ts + ` to ` + ts
	sc.Step(plan+` from the forecast with units_per_order (\d+(?:\.\d+)?)$`, func(ctx context.Context, wh, site, loc, path, start, end, units string) error {
		return w.iCreateAPlanFromTheForecast(ctx, wh, site, loc, path, start, end, "", units)
	})
	sc.Step(plan+` from the forecast of path `+ts+` with units_per_order (\d+(?:\.\d+)?)$`, w.iCreateAPlanFromTheForecast)
	sc.Step(plan+` from the forecast without units_per_order$`, func(ctx context.Context, wh, site, loc, path, start, end string) error {
		return w.iCreateAPlanFromTheForecast(ctx, wh, site, loc, path, start, end, "", "")
	})
	sc.Step(`^the capacity plan read the forecast of path `+ts+`$`, w.theCapacityPlanReadTheForecastOfPath)
	sc.Step(plan+` with assigned demand (\d+(?:\.\d+)?) labelled `+ts+`$`, w.iCreateALabelledPlan)
	sc.Step(`^the capacity plan list holds the scenarios `+ts+`$`, w.theCapacityPlanListHoldsTheScenarios)
}
