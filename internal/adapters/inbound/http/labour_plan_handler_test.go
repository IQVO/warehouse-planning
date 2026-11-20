package http_test

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	inboundhttp "github.com/claudioed/warehouse-planning/internal/adapters/inbound/http"
	outboundkafka "github.com/claudioed/warehouse-planning/internal/adapters/outbound/kafka"
	"github.com/claudioed/warehouse-planning/internal/adapters/outbound/memory"
	"github.com/claudioed/warehouse-planning/internal/application/tally"
	"github.com/claudioed/warehouse-planning/internal/application/usecases"
)

const labourPlanPath = "/labour-plans/SIM1/S1/PICK"

type labourServer struct {
	*stationServer
	outbox *memory.OutboxRepo
}

// newLabourServer wires the forecast and labour-plan routes over in-memory
// adapters, with a station tally tests can seed.
func newLabourServer(t *testing.T) *labourServer {
	t.Helper()
	forecasts, plans, ob, tallies := memory.NewWorkloadForecastRepo(), memory.NewLabourPlanRepo(), memory.NewOutboxRepo(), memory.NewStorageTallyRepo()
	uow, enc := memory.NewUnitOfWork(forecasts, plans, ob), outboundkafka.NewFanoutEncoder()
	srv := httptest.NewServer(inboundhttp.NewRouter(&inboundhttp.Server{
		ReceiveWorkloadForecast: &usecases.ReceiveWorkloadForecast{Forecasts: forecasts, Outbox: ob, Encoder: enc, UnitOfWork: uow},
		CommitLabourPlan: &usecases.CommitLabourPlan{
			Plans: plans, Forecasts: forecasts, Outbox: ob, Encoder: enc, UnitOfWork: uow, Stations: tallies,
		},
		GetLabourPlan:   &usecases.GetLabourPlan{Plans: plans, Stations: tallies},
		ListLabourPlans: &usecases.ListLabourPlans{Plans: plans, Stations: tallies},
	}))
	t.Cleanup(srv.Close)
	return &labourServer{stationServer: &stationServer{Server: srv, tally: tallies}, outbox: ob}
}

type labourPlanBody struct {
	SiteID            string   `json:"site_id"`
	ShiftID           string   `json:"shift_id"`
	PathID            string   `json:"path_id"`
	PlannedHeads      int      `json:"planned_heads"`
	RateUnitsPerHour  float64  `json:"rate_units_per_hour"`
	Hours             float64  `json:"hours"`
	InstalledStations int      `json:"installed_stations"`
	ShiftStart        *string  `json:"shift_start"`
	HeadsSource       string   `json:"heads_source"`
	Version           int64    `json:"version"`
	Warnings          []string `json:"warnings"`
}

func decodeLabourPlan(t *testing.T, body []byte) labourPlanBody {
	t.Helper()
	var p labourPlanBody
	if err := json.Unmarshal(body, &p); err != nil {
		t.Fatalf("decode plan: %v (%s)", err, body)
	}
	return p
}

func labourBody(fields ...any) map[string]any {
	body := map[string]any{"rate_units_per_hour": 10, "hours": 8, "installed_stations": 12}
	for i := 0; i+1 < len(fields); i += 2 {
		if fields[i+1] == nil {
			delete(body, fields[i].(string))
			continue
		}
		body[fields[i].(string)] = fields[i+1]
	}
	return body
}

// expectPlan checks one plan answer and the outbox size after it.
func (s *labourServer) expectPlan(t *testing.T, step string, status int, body []byte, wantStatus, wantHeads int, wantVersion int64, wantOutbox int) labourPlanBody {
	t.Helper()
	got := decodeLabourPlan(t, body)
	if status != wantStatus || got.PlannedHeads != wantHeads || got.Version != wantVersion {
		t.Fatalf("%s = %d %s, want %d with %d heads at version %d", step, status, body, wantStatus, wantHeads, wantVersion)
	}
	if n := len(s.outbox.Messages()); n != wantOutbox {
		t.Fatalf("%s: outbox = %d rows, want %d", step, n, wantOutbox)
	}
	return got
}

func TestHandler_LabourPlan_SentHeadsCreateThenIdenticalThenRevise(t *testing.T) {
	srv := newLabourServer(t)
	srv.seed(t, "SIM1-PK", tally.TypeStation, "PICK", 12)

	// The fan-out encoder writes the integration message and its analytics copy.
	status, _, body := putJSON(t, srv.Server, labourPlanPath, labourBody("planned_heads", 7))
	created := srv.expectPlan(t, "first PUT", status, body, http.StatusCreated, 7, 1, 2)
	if created.HeadsSource != "request" || created.ShiftStart != nil || created.Warnings == nil || len(created.Warnings) != 0 || created.SiteID != "SIM1" || created.ShiftID != "S1" {
		t.Fatalf("first PUT = %s", body)
	}

	status, _, body = putJSON(t, srv.Server, labourPlanPath, labourBody("planned_heads", 7))
	srv.expectPlan(t, "identical PUT", status, body, http.StatusOK, 7, 1, 2)

	status, _, body = putJSON(t, srv.Server, labourPlanPath, labourBody("planned_heads", 9))
	srv.expectPlan(t, "revising PUT", status, body, http.StatusOK, 9, 2, 4)

	status, _, body = getPath(t, srv.Server, labourPlanPath)
	srv.expectPlan(t, "GET", status, body, http.StatusOK, 9, 2, 4)
}

func TestHandler_LabourPlan_HeadsDerivedFromTheForecast(t *testing.T) {
	srv := newLabourServer(t)
	if status, _, body := putJSON(t, srv.Server, "/workload-forecasts/SIM1/PICK", buckets("2026-11-19T07:00:00Z", 500, "2026-11-19T12:00:00Z", 300)); status != http.StatusCreated {
		t.Fatalf("forecast PUT = %d %s", status, body)
	}
	status, _, body := putJSON(t, srv.Server, labourPlanPath, labourBody("shift_start", "2026-11-19T03:00:00-03:00"))
	got := decodeLabourPlan(t, body)
	// ceil(800 units / (10 units/h x 8 h)) = 10.
	if status != http.StatusCreated || got.PlannedHeads != 10 || got.HeadsSource != "forecast" || got.ShiftStart == nil || *got.ShiftStart != "2026-11-19T06:00:00Z" {
		t.Fatalf("PUT = %d %s, want 10 heads from the forecast", status, body)
	}
}

func TestHandler_LabourPlan_Problems(t *testing.T) {
	cases := map[string]struct {
		body   any
		status int
		slug   string
	}{
		"no forecast in the shift":   {labourBody("shift_start", "2026-11-19T06:00:00Z"), http.StatusUnprocessableEntity, "missing-planned-heads"},
		"no heads and no shift":      {labourBody(), http.StatusUnprocessableEntity, "missing-planned-heads"},
		"heads above stations":       {labourBody("planned_heads", 13), http.StatusUnprocessableEntity, "heads-exceed-stations"},
		"negative heads":             {labourBody("planned_heads", -1), http.StatusUnprocessableEntity, "invalid-count"},
		"zero hours":                 {labourBody("planned_heads", 1, "hours", 0), http.StatusUnprocessableEntity, "non-positive-hours"},
		"rate omitted":               {labourBody("planned_heads", 1, "rate_units_per_hour", nil), http.StatusUnprocessableEntity, "non-positive-rate"},
		"installed stations omitted": {labourBody("planned_heads", 1, "installed_stations", nil), http.StatusUnprocessableEntity, "missing-installed-stations"},
		"throughput overflows":       {labourBody("planned_heads", 1, "rate_units_per_hour", 1e308, "hours", 1e308), http.StatusUnprocessableEntity, "throughput-not-finite"},
		"malformed shift start":      {labourBody("shift_start", "06:00"), http.StatusBadRequest, "malformed-shift-start"},
		"fractional heads":           {labourBody("planned_heads", 1.5), http.StatusBadRequest, "malformed-json"},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			srv := newLabourServer(t)
			status, hdr, body := putJSON(t, srv.Server, labourPlanPath, tc.body)
			if status != tc.status || hdr.Get("Content-Type") != "application/problem+json" || problemType(t, body) != tc.slug {
				t.Fatalf("PUT = %d %s %s, want %d %s", status, hdr.Get("Content-Type"), body, tc.status, tc.slug)
			}
			if n := len(srv.outbox.Messages()); n != 0 {
				t.Fatalf("outbox = %d, want 0", n)
			}
		})
	}
}

func TestHandler_LabourPlan_StationWarningKeepsTheNumber(t *testing.T) {
	srv := newLabourServer(t)
	srv.seed(t, "SIM1-PK", tally.TypeStation, "PICK", 3)
	status, _, body := putJSON(t, srv.Server, labourPlanPath, labourBody("planned_heads", 4, "installed_stations", 5))
	got := decodeLabourPlan(t, body)
	if status != http.StatusCreated || got.InstalledStations != 5 || len(got.Warnings) != 1 || !strings.Contains(got.Warnings[0], "knows 3 PICK station(s)") {
		t.Fatalf("PUT = %d %s, want the plan stored with a station warning", status, body)
	}
	if status, _, body = getPath(t, srv.Server, labourPlanPath); len(decodeLabourPlan(t, body).Warnings) != 1 {
		t.Fatalf("GET = %d %s, want the warning on the read too", status, body)
	}
}

func TestHandler_LabourPlan_UnknownPlanIs404(t *testing.T) {
	srv := newLabourServer(t)
	status, _, body := getPath(t, srv.Server, labourPlanPath)
	if status != http.StatusNotFound || problemType(t, body) != "labour-plan-not-found" {
		t.Fatalf("GET = %d %s, want 404 labour-plan-not-found", status, body)
	}
}

func TestHandler_LabourPlan_ListPagesByPathID(t *testing.T) {
	srv := newLabourServer(t)
	for _, p := range []string{"/labour-plans/SIM1/S1/SORT", "/labour-plans/SIM1/S1/PACK", labourPlanPath, "/labour-plans/SIM1/S2/PICK"} {
		if status, _, body := putJSON(t, srv.Server, p, labourBody("planned_heads", 1)); status != http.StatusCreated {
			t.Fatalf("PUT %s = %d %s", p, status, body)
		}
	}
	var first struct {
		Items      []labourPlanBody `json:"items"`
		NextCursor string           `json:"nextCursor"`
	}
	status, _, body := getPath(t, srv.Server, "/labour-plans?site_id=SIM1&shift_id=S1&limit=2")
	if err := json.Unmarshal(body, &first); err != nil || status != http.StatusOK || len(first.Items) != 2 ||
		first.Items[0].PathID != "PACK" || first.Items[1].PathID != "PICK" || first.NextCursor == "" {
		t.Fatalf("first page = %d %s", status, body)
	}
	var rest struct {
		Items      []labourPlanBody `json:"items"`
		NextCursor string           `json:"nextCursor"`
	}
	status, _, body = getPath(t, srv.Server, "/labour-plans?site_id=SIM1&shift_id=S1&limit=2&cursor="+first.NextCursor)
	if err := json.Unmarshal(body, &rest); err != nil || status != http.StatusOK || len(rest.Items) != 1 || rest.Items[0].PathID != "SORT" || rest.NextCursor != "" {
		t.Fatalf("second page = %d %s", status, body)
	}
}

func TestHandler_LabourPlan_ListProblems(t *testing.T) {
	srv := newLabourServer(t)
	for target, slug := range map[string]string{
		"/labour-plans?shift_id=S1":                                "missing-site-id",
		"/labour-plans?site_id=SIM1":                               "missing-shift-id",
		"/labour-plans?site_id=SIM1&shift_id=S1&x=1":               "unknown-query-parameter",
		"/labour-plans?site_id=SIM1&shift_id=S1&limit=0":           "invalid-limit",
		"/labour-plans?site_id=SIM1&shift_id=S1&cursor=not-a-page": "invalid-cursor",
	} {
		status, _, body := getPath(t, srv.Server, target)
		if status != http.StatusBadRequest || problemType(t, body) != slug {
			t.Fatalf("GET %s = %d %s, want 400 %s", target, status, body, slug)
		}
	}
}
