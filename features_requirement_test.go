package main_test

import (
	"context"
	"fmt"
	"net/http"
	"strconv"
	"strings"

	"github.com/cucumber/godog"

	inboundhttp "github.com/claudioed/warehouse-planning/internal/adapters/inbound/http"
	"github.com/claudioed/warehouse-planning/internal/adapters/outbound/memory"
	"github.com/claudioed/warehouse-planning/internal/application/ports"
	"github.com/claudioed/warehouse-planning/internal/application/usecases"
)

// wireRequirementSide adds the docs/adr/0023 use cases to the BDD server the
// way cmd/api's wireRequirementSide does: in-memory forecast and labour-plan
// repositories, the world's outbox and encoder, the facility tally the
// facility-layout consumer feeds (the station warning's reader) and the
// committed heads the labor consumer writes (the plan trigger of the labour
// plan drift, decision 3). It returns the labour plans for wireDrift.
func wireRequirementSide(s *inboundhttp.Server, ob *memory.OutboxRepo, encoder ports.EventEncoder, stations ports.StorageTallyReader, commitments *memory.CommittedHeadsRepo) *memory.LabourPlanRepo {
	forecasts, plans := memory.NewWorkloadForecastRepo(), memory.NewLabourPlanRepo()
	uow := memory.NewUnitOfWork(forecasts, plans, ob, commitments)
	s.ReceiveWorkloadForecast = &usecases.ReceiveWorkloadForecast{Forecasts: forecasts, Outbox: ob, Encoder: encoder, UnitOfWork: uow}
	s.GetWorkloadForecast = &usecases.GetWorkloadForecast{Forecasts: forecasts}
	s.CommitLabourPlan = &usecases.CommitLabourPlan{
		Plans: plans, Forecasts: forecasts, Outbox: ob, Encoder: encoder, UnitOfWork: uow, Stations: stations,
		Commitments: commitments,
	}
	s.GetLabourPlan = &usecases.GetLabourPlan{Plans: plans, Stations: stations}
	s.ListLabourPlans = &usecases.ListLabourPlans{Plans: plans, Stations: stations}
	return plans
}

// iReceiveAWorkloadForecast PUTs the forecast; buckets is "cpt=quantity, ...".
func (w *world) iReceiveAWorkloadForecast(ctx context.Context, pathID, siteID, raw string) error {
	buckets := make([]map[string]any, 0)
	for _, pair := range strings.Split(raw, ",") {
		cpt, qty, ok := strings.Cut(strings.TrimSpace(pair), "=")
		if !ok {
			return fmt.Errorf("bucket %q is not cpt=quantity", pair)
		}
		n, err := strconv.ParseInt(qty, 10, 64)
		if err != nil {
			return err
		}
		buckets = append(buckets, map[string]any{"cpt": cpt, "quantity": n})
	}
	return w.record(ctx, http.MethodPut, "/workload-forecasts/"+siteID+"/"+pathID, map[string]any{"buckets": buckets})
}

func (w *world) theWorkloadForecastIs(version, total int64) error {
	var body struct {
		Version       int64 `json:"version"`
		TotalQuantity int64 `json:"total_quantity"`
	}
	if err := w.decode(&body); err != nil {
		return err
	}
	if body.Version != version || body.TotalQuantity != total {
		return fmt.Errorf("expected the forecast at version %d with total %d, got %s", version, total, string(w.body))
	}
	return nil
}

func labourPlanPath(pathID, shiftID, siteID string) string {
	return "/labour-plans/" + siteID + "/" + shiftID + "/" + pathID
}

// theResponsePathIDIs checks the path_id of a forecast or labour plan
// response: the canonical path code (trimmed, upper case), whatever case the
// request used.
func (w *world) theResponsePathIDIs(want string) error {
	var body struct {
		PathID string `json:"path_id"`
	}
	if err := w.decode(&body); err != nil {
		return err
	}
	if body.PathID != want {
		return fmt.Errorf("expected path_id %q, got %s", want, string(w.body))
	}
	return nil
}

func (w *world) iCommitALabourPlanWithHeads(ctx context.Context, pathID, shiftID, siteID string, heads int, rate, hours float64, stations int) error {
	return w.record(ctx, http.MethodPut, labourPlanPath(pathID, shiftID, siteID), map[string]any{
		"planned_heads": heads, "rate_units_per_hour": rate, "hours": hours, "installed_stations": stations,
	})
}

func (w *world) iCommitALabourPlanWithoutHeads(ctx context.Context, pathID, shiftID, siteID string, rate, hours float64, stations int, shiftStart string) error {
	return w.record(ctx, http.MethodPut, labourPlanPath(pathID, shiftID, siteID), map[string]any{
		"rate_units_per_hour": rate, "hours": hours, "installed_stations": stations, "shift_start": shiftStart,
	})
}

func (w *world) theLabourPlanIs(heads int, source string, version int64) error {
	var body struct {
		PlannedHeads int    `json:"planned_heads"`
		HeadsSource  string `json:"heads_source"`
		Version      int64  `json:"version"`
	}
	if err := w.decode(&body); err != nil {
		return err
	}
	if body.PlannedHeads != heads || body.HeadsSource != source || body.Version != version {
		return fmt.Errorf("expected %d heads from %s at version %d, got %s", heads, source, version, string(w.body))
	}
	return nil
}

func (w *world) iListTheLabourPlans(ctx context.Context, shiftID, siteID string) error {
	return w.record(ctx, http.MethodGet, "/labour-plans?site_id="+siteID+"&shift_id="+shiftID, nil)
}

func (w *world) theLabourPlanListHolds(raw string) error {
	var body struct {
		Items []struct {
			PathID string `json:"path_id"`
		} `json:"items"`
	}
	if err := w.decode(&body); err != nil {
		return err
	}
	got := make([]string, 0, len(body.Items))
	for _, item := range body.Items {
		got = append(got, item.PathID)
	}
	if strings.Join(got, ", ") != raw {
		return fmt.Errorf("expected the paths %q, got %q", raw, strings.Join(got, ", "))
	}
	return nil
}

// theOutboxHoldsEvents counts the queued messages of one event name (the
// last segment of the CloudEvents type).
func (w *world) theOutboxHoldsEvents(n int, name string) error {
	got := 0
	for _, m := range w.outbox.Messages() {
		if strings.HasSuffix(m.EventType, "."+name) {
			got++
		}
	}
	if got != n {
		return fmt.Errorf("expected %d %s message(s) in the outbox, got %d", n, name, got)
	}
	return nil
}

func registerRequirementSteps(sc *godog.ScenarioContext, w *world) {
	sc.Step(`^I receive a workload forecast for path "([^"]*)" at site "([^"]*)" with buckets "([^"]*)"$`, w.iReceiveAWorkloadForecast)
	sc.Step(`^the workload forecast is at version (\d+) with total quantity (\d+)$`, w.theWorkloadForecastIs)
	sc.Step(`^I commit a labour plan for path "([^"]*)" in shift "([^"]*)" at site "([^"]*)" with (-?\d+) planned heads, rate (\d+(?:\.\d+)?) units per hour, (\d+(?:\.\d+)?) hours and (\d+) installed stations$`, w.iCommitALabourPlanWithHeads)
	sc.Step(`^I commit a labour plan for path "([^"]*)" in shift "([^"]*)" at site "([^"]*)" without planned heads, rate (\d+(?:\.\d+)?) units per hour, (\d+(?:\.\d+)?) hours, (\d+) installed stations and shift start "([^"]*)"$`, w.iCommitALabourPlanWithoutHeads)
	sc.Step(`^the labour plan has (\d+) planned heads from "(request|forecast)" at version (\d+)$`, w.theLabourPlanIs)
	sc.Step(`^I list the labour plans of shift "([^"]*)" at site "([^"]*)"$`, w.iListTheLabourPlans)
	sc.Step(`^the labour plan list holds the paths "([^"]*)"$`, w.theLabourPlanListHolds)
	sc.Step(`^the outbox holds (\d+) ([A-Za-z]+) events?$`, w.theOutboxHoldsEvents)
	sc.Step(`^the response path id is "([^"]*)"$`, w.theResponsePathIDIs)
}
