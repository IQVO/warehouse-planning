package http_test

import (
	"encoding/json"
	"net/http"
	"net/url"
	"strings"
	"testing"
)

func scenarioBody(site, start, end, scenario string) map[string]any {
	b := planBody(100)
	b["site_id"], b["window_start"], b["window_end"] = site, start, end
	if scenario != "" {
		b["scenario"] = scenario
	}
	return b
}

// listedScenarios renders the scenario labels of a list page, in order.
func listedScenarios(t *testing.T, body []byte) []string {
	t.Helper()
	var page struct {
		Items []struct {
			Scenario string `json:"scenario"`
		} `json:"items"`
	}
	if err := json.Unmarshal(body, &page); err != nil {
		t.Fatalf("decode page: %v (%s)", err, body)
	}
	out := make([]string, 0, len(page.Items))
	for _, i := range page.Items {
		out = append(out, i.Scenario)
	}
	return out
}

func TestHandler_CreateCapacityPlan_ScenarioLabel(t *testing.T) {
	srv, _ := newForecastPlanServer(t)
	status, _, body := postJSON(t, srv, "/capacity-plans", scenarioBody("SIM1", planStart, planEnd, "second pack line open"))
	if status != http.StatusCreated {
		t.Fatalf("create = %d: %s", status, body)
	}
	var got map[string]any
	_ = json.Unmarshal(body, &got)
	if got["scenario"] != "second pack line open" || got["status"] != "DRAFT" {
		t.Fatalf("plan = %s", body)
	}
	req, _ := http.NewRequest(http.MethodGet, srv.URL+"/capacity-plans/"+got["id"].(string), http.NoBody)
	if status, _, body = doRequest(t, req); status != http.StatusOK || !strings.Contains(string(body), `"scenario":"second pack line open"`) {
		t.Fatalf("get = %d: %s", status, body)
	}

	status, _, body = postJSON(t, srv, "/capacity-plans", planBody(100))
	var unlabelled map[string]any
	_ = json.Unmarshal(body, &unlabelled)
	if _, present := unlabelled["scenario"]; status != http.StatusCreated || present {
		t.Fatalf("an unlabelled plan = %d %s, want no scenario field", status, body)
	}

	for name, label := range map[string]string{"empty": "", "65 characters": strings.Repeat("x", 65)} {
		b := planBody(100)
		b["scenario"] = label
		status, _, body := postJSON(t, srv, "/capacity-plans", b)
		if status != http.StatusUnprocessableEntity || problemType(t, body) != "invalid-scenario" {
			t.Errorf("%s scenario: %d %s, want 422 invalid-scenario", name, status, body)
		}
	}
}

// GET /capacity-plans filters by site_id, path_id, window_start and
// window_end additively, newest first; location keeps working.
func TestHandler_ListCapacityPlans_AdditiveFilters(t *testing.T) {
	srv, _ := newForecastPlanServer(t)
	const shortEnd = "2026-10-05T12:00:00Z"
	for _, b := range []map[string]any{
		scenarioBody("SIM1", planStart, planEnd, "baseline"),
		scenarioBody("SIM2", planStart, planEnd, "other site"),
		scenarioBody("SIM1", planStart, shortEnd, "short window"),
		scenarioBody("SIM1", planStart, planEnd, "+15% volume"),
	} {
		if status, _, body := postJSON(t, srv, "/capacity-plans", b); status != http.StatusCreated {
			t.Fatalf("create = %d: %s", status, body)
		}
	}
	for _, c := range []struct {
		query string
		want  string
	}{
		{"", "[+15% volume short window other site baseline]"},
		{"location=PATH-ZONE-A", "[+15% volume short window other site baseline]"},
		{"site_id=SIM1", "[+15% volume short window baseline]"},
		{"site_id=SIM1&path_id=pick-rebin-pack&window_start=" + planStart + "&window_end=" + url.QueryEscape(planEnd), "[+15% volume baseline]"},
		{"window_end=" + shortEnd, "[short window]"},
		{"window_start=2026-10-05T05:00:00-03:00", "[+15% volume short window other site baseline]"},
		{"path_id=other", "[]"},
		{"site_id=", "[+15% volume short window other site baseline]"},
	} {
		status, _, body := getPath(t, srv, "/capacity-plans?"+strings.ReplaceAll(c.query, "+", "%2B"))
		if status != http.StatusOK {
			t.Fatalf("%q = %d: %s", c.query, status, body)
		}
		if got := "[" + strings.Join(listedScenarios(t, body), " ") + "]"; got != c.want {
			t.Errorf("%q lists %s, want %s", c.query, got, c.want)
		}
	}
}

func TestHandler_ListCapacityPlans_ValidatesTheFilters(t *testing.T) {
	srv, _ := newForecastPlanServer(t)
	for query, slug := range map[string]string{
		"window_start=yesterday":                                 "malformed-window-start",
		"window_end=2026-10-05":                                  "malformed-window-end",
		"window_start=" + planEnd + "&window_end=" + planStart:   "invalid-capacity-window",
		"window_start=" + planStart + "&window_end=" + planStart: "invalid-capacity-window",
		"scenario=baseline":                                      "unknown-query-parameter",
	} {
		status, _, body := getPath(t, srv, "/capacity-plans?"+query)
		if status != http.StatusBadRequest || problemType(t, body) != slug {
			t.Errorf("%q = %d %s, want 400 %s", query, status, body, slug)
		}
	}
}
