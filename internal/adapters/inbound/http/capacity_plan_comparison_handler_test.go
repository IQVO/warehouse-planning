package http_test

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	inboundhttp "github.com/claudioed/warehouse-planning/internal/adapters/inbound/http"
	"github.com/claudioed/warehouse-planning/internal/adapters/outbound/memory"
	"github.com/claudioed/warehouse-planning/internal/application/usecases"
)

// createdPlanID POSTs a plan body and returns the id of the 201 plan.
func createdPlanID(t *testing.T, srv *httptest.Server, body map[string]any) string {
	t.Helper()
	status, _, resp := postJSON(t, srv, "/capacity-plans", body)
	if status != http.StatusCreated {
		t.Fatalf("create = %d: %s", status, resp)
	}
	var plan struct {
		ID string `json:"id"`
	}
	if err := json.Unmarshal(resp, &plan); err != nil {
		t.Fatal(err)
	}
	return plan.ID
}

// newComparisonServer is the forecast plan server with the comparison read
// wired over the same plan store.
func newComparisonServer(t *testing.T) *httptest.Server {
	t.Helper()
	srv, _ := newForecastPlanServer(t)
	return srv
}

func TestHandler_CompareCapacityPlans_LaysStoredPlansSideBySide(t *testing.T) {
	srv := newComparisonServer(t)
	baseline := createdPlanID(t, srv, scenarioBody("SIM1", planStart, planEnd, "baseline"))
	higher := planBody(9200)
	higher["scenario"] = "+15% volume"
	more := createdPlanID(t, srv, higher)

	status, _, body := getPath(t, srv, "/capacity-plans/comparisons?ids="+more+","+baseline)
	if status != http.StatusOK {
		t.Fatalf("compare = %d: %s", status, body)
	}
	var got struct {
		SiteID      string `json:"site_id"`
		WindowStart string `json:"window_start"`
		WindowEnd   string `json:"window_end"`
		Items       []struct {
			PlanID               string           `json:"plan_id"`
			Scenario             string           `json:"scenario"`
			Status               string           `json:"status"`
			AssignedDemand       float64          `json:"assigned_demand"`
			DemandSource         string           `json:"demand_source"`
			PathCapacity         float64          `json:"path_capacity"`
			CapacityOverWindow   float64          `json:"capacity_over_window"`
			Shortage             float64          `json:"shortage"`
			BottleneckStep       string           `json:"bottleneck_step"`
			BottleneckConstraint string           `json:"bottleneck_constraint"`
			LabourRequirement    []map[string]any `json:"labour_requirement"`
		} `json:"items"`
	}
	if err := json.Unmarshal(body, &got); err != nil {
		t.Fatal(err)
	}
	rendered := make([]string, 0, len(got.Items))
	for _, i := range got.Items {
		rendered = append(rendered, fmt.Sprintf("%v %s %s demand=%v %s cap=%v over=%v short=%v %s/%s steps=%d",
			i.PlanID == more || i.PlanID == baseline, i.Scenario, i.Status, i.AssignedDemand, i.DemandSource,
			i.PathCapacity, i.CapacityOverWindow, i.Shortage, i.BottleneckStep, i.BottleneckConstraint, len(i.LabourRequirement)))
	}
	want := []string{
		"true +15% volume DRAFT demand=9200 request cap=1000 over=8000 short=1200 REBIN/LABOR steps=3",
		"true baseline DRAFT demand=100 request cap=1000 over=8000 short=0 REBIN/LABOR steps=3",
	}
	header := got.SiteID + " " + got.WindowStart + " " + got.WindowEnd
	if header != "SIM1 "+planStart+" "+planEnd || strings.Join(rendered, "\n") != strings.Join(want, "\n") || got.Items[0].PlanID != more {
		t.Fatalf("comparison = %s\n%s", header, strings.Join(rendered, "\n"))
	}
}

func TestHandler_CompareCapacityPlans_Problems(t *testing.T) {
	srv := newComparisonServer(t)
	a := createdPlanID(t, srv, scenarioBody("SIM1", planStart, planEnd, "a"))
	b := createdPlanID(t, srv, scenarioBody("SIM1", planStart, planEnd, "b"))
	otherSite := createdPlanID(t, srv, scenarioBody("SIM2", planStart, planEnd, "c"))
	otherWindow := createdPlanID(t, srv, scenarioBody("SIM1", planStart, "2026-10-05T12:00:00Z", "d"))
	for name, c := range map[string]struct {
		query  string
		status int
		slug   string
	}{
		"no ids":         {"", http.StatusUnprocessableEntity, "invalid-comparison-ids"},
		"one id":         {"ids=" + a, http.StatusUnprocessableEntity, "invalid-comparison-ids"},
		"six ids":        {"ids=" + strings.Join([]string{a, b, otherSite, otherWindow, "e", "f"}, ","), http.StatusUnprocessableEntity, "invalid-comparison-ids"},
		"a repeated id":  {"ids=" + a + "," + a, http.StatusUnprocessableEntity, "invalid-comparison-ids"},
		"a blank id":     {"ids=" + a + ",", http.StatusUnprocessableEntity, "invalid-comparison-ids"},
		"another site":   {"ids=" + a + "," + otherSite, http.StatusUnprocessableEntity, "comparison-site-mismatch"},
		"another window": {"ids=" + a + "," + b + "," + otherWindow, http.StatusUnprocessableEntity, "comparison-window-mismatch"},
		"an unknown id":  {"ids=" + a + ",nope", http.StatusNotFound, "capacity-plan-not-found"},
		"another param":  {"ids=" + a + "," + b + "&limit=2", http.StatusBadRequest, "unknown-query-parameter"},
	} {
		status, _, body := getPath(t, srv, "/capacity-plans/comparisons?"+c.query)
		if status != c.status || problemType(t, body) != c.slug {
			t.Errorf("%s: %d %s, want %d %s", name, status, body, c.status, c.slug)
		}
	}
}

// Without the use case wired the route does not exist, and the word
// "comparisons" is still never read as a plan id.
func TestHandler_CompareCapacityPlans_RouteIsNotAPlanID(t *testing.T) {
	plans := memory.NewCapacityPlanRepo()
	srv := httptest.NewServer(inboundhttp.NewRouter(&inboundhttp.Server{
		CapacityPlans:        plans,
		CompareCapacityPlans: &usecases.CompareCapacityPlans{Plans: plans},
	}))
	t.Cleanup(srv.Close)
	status, _, body := getPath(t, srv, "/capacity-plans/comparisons?ids=x,y")
	if status != http.StatusNotFound || problemType(t, body) != "capacity-plan-not-found" || !strings.Contains(string(body), "x") {
		t.Fatalf("compare of unknown ids = %d %s, want the comparison's 404 naming x", status, body)
	}
	status, _, body = getPath(t, srv, "/capacity-plans/comparisons")
	if status != http.StatusUnprocessableEntity {
		t.Fatalf("GET /capacity-plans/comparisons without ids = %d %s: it reached GET /capacity-plans/{id}", status, body)
	}
}
