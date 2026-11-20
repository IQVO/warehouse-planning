package http_test

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	inboundhttp "github.com/claudioed/warehouse-planning/internal/adapters/inbound/http"
	outboundkafka "github.com/claudioed/warehouse-planning/internal/adapters/outbound/kafka"
	"github.com/claudioed/warehouse-planning/internal/adapters/outbound/memory"
	"github.com/claudioed/warehouse-planning/internal/application/usecases"
	"github.com/claudioed/warehouse-planning/internal/domain/workloadforecast"
)

// newForecastPlanServer is newPlanServer with a workload forecast store wired
// into CreateCapacityPlan (docs/adr/0024 decision 1), seeded with the plan
// fixture's capacities (8000 orders over the 8h window).
func newForecastPlanServer(t *testing.T) (*httptest.Server, *memory.WorkloadForecastRepo) {
	t.Helper()
	pcs, paths, plans, ob := memory.NewProcessCapacityRepo(), memory.NewProcessPathRepo(), memory.NewCapacityPlanRepo(), memory.NewOutboxRepo()
	forecasts := memory.NewWorkloadForecastRepo()
	uow := memory.NewUnitOfWork(pcs, plans, ob)
	pathCapacity := &usecases.GetProcessPathCapacity{ProcessPaths: paths, ProcessCapacities: pcs, StationStandards: memory.NewStationStandardRepo(), Tally: memory.NewStorageTallyRepo()}
	clock := time.Date(2026, 10, 4, 12, 0, 0, 0, time.UTC)
	s := &inboundhttp.Server{
		RegisterProcessCapacityConstraint: &usecases.RegisterProcessCapacityConstraint{Repo: pcs},
		ProcessCapacities:                 pcs,
		RegisterProcessPath:               &usecases.RegisterProcessPath{Repo: paths},
		GetProcessPathCapacity:            pathCapacity,
		CreateCapacityPlan: &usecases.CreateCapacityPlan{
			PathCapacity: pathCapacity, Plans: plans, Outbox: ob, Encoder: outboundkafka.NewEncoder(), UnitOfWork: uow,
			Forecasts: forecasts,
			// One minute apart, so "newest first" never hangs on a clock tie.
			Now: func() time.Time { clock = clock.Add(time.Minute); return clock },
		},
		CapacityPlans:        plans,
		ListCapacityPlans:    &usecases.ListCapacityPlans{Plans: plans},
		CompareCapacityPlans: &usecases.CompareCapacityPlans{Plans: plans},
	}
	srv := httptest.NewServer(inboundhttp.NewRouter(s))
	t.Cleanup(srv.Close)
	seedPlanFixture(t, srv)
	return srv, forecasts
}

func seedHTTPForecast(t *testing.T, repo *memory.WorkloadForecastRepo, path string, buckets map[string]int64) {
	t.Helper()
	var bs []workloadforecast.Bucket
	for cpt, qty := range buckets {
		at, err := time.Parse(time.RFC3339, cpt)
		if err != nil {
			t.Fatal(err)
		}
		bs = append(bs, workloadforecast.Bucket{CPT: at, Quantity: qty})
	}
	f, _, err := workloadforecast.New("SIM1", path, bs, time.Date(2026, 10, 4, 6, 0, 0, 0, time.UTC))
	if err != nil {
		t.Fatal(err)
	}
	if err := repo.Save(context.Background(), f); err != nil {
		t.Fatal(err)
	}
}

func forecastPlanBody(extra map[string]any) map[string]any {
	b := planBody(nil)
	b["demand_source"] = "forecast"
	for k, v := range extra {
		b[k] = v
	}
	return b
}

func TestHandler_CreateCapacityPlan_FromTheForecast(t *testing.T) {
	srv, forecasts := newForecastPlanServer(t)
	// 20000 units on window_start (in) + 3 units inside, the window_end bucket out:
	// ceil(20003 / 2.5) = 8002 orders against 8000 of capacity.
	seedHTTPForecast(t, forecasts, "PICK-REBIN-PACK", map[string]int64{planStart: 20000, "2026-10-05T11:00:00Z": 3, planEnd: 999})
	status, _, body := postJSON(t, srv, "/capacity-plans", forecastPlanBody(nil))
	if status != http.StatusCreated {
		t.Fatalf("create = %d: %s", status, body)
	}
	var got map[string]any
	if err := json.Unmarshal(body, &got); err != nil {
		t.Fatal(err)
	}
	if got["assigned_demand"] != 8002.0 || got["shortage"] != 2.0 || got["demand_source"] != "forecast" || got["forecast_path_id"] != "PICK-REBIN-PACK" {
		t.Fatalf("plan = %s", body)
	}

	req, _ := http.NewRequest(http.MethodGet, srv.URL+"/capacity-plans/"+got["id"].(string), http.NoBody)
	status, _, body = doRequest(t, req)
	if status != http.StatusOK {
		t.Fatalf("get = %d: %s", status, body)
	}
	var stored map[string]any
	_ = json.Unmarshal(body, &stored)
	if stored["demand_source"] != "forecast" || stored["forecast_path_id"] != "PICK-REBIN-PACK" {
		t.Fatalf("stored plan lost its forecast provenance: %s", body)
	}
}

func TestHandler_CreateCapacityPlan_ForecastPathIDOtherThanPathID(t *testing.T) {
	srv, forecasts := newForecastPlanServer(t)
	seedHTTPForecast(t, forecasts, "PICK", map[string]int64{"2026-10-05T09:00:00Z": 101})
	status, _, body := postJSON(t, srv, "/capacity-plans", forecastPlanBody(map[string]any{"forecast_path_id": "pick"}))
	if status != http.StatusCreated {
		t.Fatalf("create = %d: %s", status, body)
	}
	var got map[string]any
	_ = json.Unmarshal(body, &got)
	if got["assigned_demand"] != 41.0 || got["forecast_path_id"] != "PICK" || got["path_id"] != "pick-rebin-pack" {
		t.Fatalf("plan = %s", body)
	}
}

func TestHandler_CreateCapacityPlan_NonForecastPlanOmitsForecastPathID(t *testing.T) {
	srv, _ := newForecastPlanServer(t)
	status, _, body := postJSON(t, srv, "/capacity-plans", planBody(10))
	if status != http.StatusCreated {
		t.Fatalf("create = %d: %s", status, body)
	}
	var got map[string]any
	_ = json.Unmarshal(body, &got)
	if _, present := got["forecast_path_id"]; present || got["demand_source"] != "request" {
		t.Fatalf("a request plan = %s", body)
	}
}

func TestHandler_CreateCapacityPlan_ForecastProblems(t *testing.T) {
	srv, forecasts := newForecastPlanServer(t)
	seedHTTPForecast(t, forecasts, "PICK-REBIN-PACK", map[string]int64{planEnd: 50})
	noUnits := forecastPlanBody(nil)
	delete(noUnits, "units_per_order")
	stated := forecastPlanBody(map[string]any{"assigned_demand": 5})
	pathWithoutForecast := planBody(5)
	pathWithoutForecast["forecast_path_id"] = "PICK"
	for name, c := range map[string]struct {
		body map[string]any
		slug string
	}{
		"only a bucket on window_end":            {forecastPlanBody(nil), "missing-assigned-demand"},
		"no forecast for the named path":         {forecastPlanBody(map[string]any{"forecast_path_id": "PACK"}), "missing-assigned-demand"},
		"units_per_order missing":                {noUnits, "missing-units-per-order"},
		"units_per_order zero":                   {forecastPlanBody(map[string]any{"units_per_order": 0}), "non-positive-conversion-factor"},
		"unknown demand_source":                  {forecastPlanBody(map[string]any{"demand_source": "inbound"}), "unknown-demand-source"},
		"forecast with a stated assigned_demand": {stated, "demand-source-conflict"},
		"forecast_path_id without forecast":      {pathWithoutForecast, "demand-source-conflict"},
	} {
		status, _, body := postJSON(t, srv, "/capacity-plans", c.body)
		if status != http.StatusUnprocessableEntity || problemType(t, body) != c.slug {
			t.Errorf("%s: %d %s, want 422 %s", name, status, body, c.slug)
		}
	}
}
