package main_test

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/cucumber/godog"
)

// nextPlanTime is CreateCapacityPlan's clock for the scenarios: each plan is
// created one minute after the previous one.
func (w *world) nextPlanTime() time.Time {
	w.planClock = w.planClock.Add(time.Minute)
	return w.planClock
}

func (w *world) iListTheProcessPaths(ctx context.Context) error {
	return w.record(ctx, http.MethodGet, "/process-paths", nil)
}

func (w *world) iListTheCapacityPlans(ctx context.Context, location string) error {
	return w.record(ctx, http.MethodGet, "/capacity-plans?location="+location, nil)
}

func (w *world) iListTheCapacityPlansWithLimit(ctx context.Context, location, limit string) error {
	return w.record(ctx, http.MethodGet, "/capacity-plans?location="+location+"&limit="+limit, nil)
}

func (w *world) iListTheCapacityPlansOfEveryLocation(ctx context.Context) error {
	return w.record(ctx, http.MethodGet, "/capacity-plans", nil)
}

func (w *world) theProcessPathListIs(raw string) error {
	var body struct {
		ProcessPaths []struct {
			ID    string   `json:"id"`
			Steps []string `json:"steps"`
		} `json:"process_paths"`
	}
	if err := w.decode(&body); err != nil {
		return err
	}
	if body.ProcessPaths == nil {
		return fmt.Errorf("process_paths must be an array, never null: %s", string(w.body))
	}
	got := make([]string, 0, len(body.ProcessPaths))
	for _, p := range body.ProcessPaths {
		got = append(got, p.ID+"="+strings.Join(p.Steps, ">"))
	}
	want := make([]string, 0)
	for _, item := range strings.Split(raw, ",") {
		if item = strings.TrimSpace(item); item != "" {
			want = append(want, item)
		}
	}
	if strings.Join(got, "|") != strings.Join(want, "|") {
		return fmt.Errorf("expected process paths %v, got %v", want, got)
	}
	return nil
}

func (w *world) theProcessPathListIsEmpty() error {
	return w.theProcessPathListIs("")
}

// theCapacityPlanListHasDemands asserts the listed plans' assigned_demand in
// order (newest first) and that every item carries the single-plan shape's
// key fields.
func (w *world) theCapacityPlanListHasDemands(raw string) error {
	var body struct {
		CapacityPlans []map[string]any `json:"capacity_plans"`
	}
	if err := w.decode(&body); err != nil {
		return err
	}
	if body.CapacityPlans == nil {
		return fmt.Errorf("capacity_plans must be an array, never null: %s", string(w.body))
	}
	var got, want []string
	for _, p := range body.CapacityPlans {
		for _, key := range []string{"id", "status", "shortage", "bottleneck_step", "demand_source", "warnings"} {
			if _, ok := p[key]; !ok {
				return fmt.Errorf("listed plan lacks %q: %v", key, p)
			}
		}
		got = append(got, strconv.FormatFloat(p["assigned_demand"].(float64), 'f', -1, 64))
	}
	for _, item := range strings.Split(raw, ",") {
		if item = strings.TrimSpace(item); item != "" {
			want = append(want, item)
		}
	}
	if strings.Join(got, "|") != strings.Join(want, "|") {
		return fmt.Errorf("expected assigned demands %v (newest first), got %v", want, got)
	}
	return nil
}

func (w *world) theCapacityPlanListIsEmpty() error {
	return w.theCapacityPlanListHasDemands("")
}

func (w *world) theListedPlanIs(index int, status string) error {
	var body struct {
		CapacityPlans []struct {
			Status      string  `json:"status"`
			PublishedAt *string `json:"published_at"`
		} `json:"capacity_plans"`
	}
	if err := json.Unmarshal(w.body, &body); err != nil {
		return err
	}
	if index < 1 || index > len(body.CapacityPlans) {
		return fmt.Errorf("no listed plan #%d in %s", index, string(w.body))
	}
	p := body.CapacityPlans[index-1]
	if p.Status != status || (status == "PUBLISHED") != (p.PublishedAt != nil) {
		return fmt.Errorf("listed plan #%d is %s (published_at %v), want %s", index, p.Status, p.PublishedAt, status)
	}
	return nil
}

// iCreateTheCapacityPlanWithDemand creates a plan on the list scenarios' fixed
// path and window, remembering its id for a later publish.
func (w *world) iCreateAPlanWithDemandAt(ctx context.Context, demand float64, location string) error {
	return w.iCreateACapacityPlan(ctx, "WH-1", location, "pick-rebin-pack", "2026-10-05T08:00:00Z", "2026-10-05T16:00:00Z", demand, 2.5, 1)
}

func registerListSteps(sc *godog.ScenarioContext, w *world) {
	sc.Step(`^I list the process paths$`, w.iListTheProcessPaths)
	sc.Step(`^I list the capacity plans for location "([^"]*)"$`, w.iListTheCapacityPlans)
	sc.Step(`^I list the capacity plans for location "([^"]*)" with limit "([^"]*)"$`, w.iListTheCapacityPlansWithLimit)
	sc.Step(`^I list the capacity plans of every location$`, w.iListTheCapacityPlansOfEveryLocation)
	sc.Step(`^the process path list is "([^"]*)"$`, w.theProcessPathListIs)
	sc.Step(`^the process path list is empty$`, w.theProcessPathListIsEmpty)
	sc.Step(`^the capacity plan list has assigned demands ([0-9, ]+)$`, w.theCapacityPlanListHasDemands)
	sc.Step(`^the capacity plan list is empty$`, w.theCapacityPlanListIsEmpty)
	sc.Step(`^listed plan (\d+) is (DRAFT|PUBLISHED)$`, w.theListedPlanIs)
	sc.Step(`^I create a capacity plan with assigned demand (\d+) at "([^"]*)" on path "pick-rebin-pack"$`, w.iCreateAPlanWithDemandAt)
}
