package http_test

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strconv"
	"testing"

	inboundhttp "github.com/claudioed/warehouse-planning/internal/adapters/inbound/http"
	outboundkafka "github.com/claudioed/warehouse-planning/internal/adapters/outbound/kafka"
	"github.com/claudioed/warehouse-planning/internal/adapters/outbound/memory"
	"github.com/claudioed/warehouse-planning/internal/application/tally"
	"github.com/claudioed/warehouse-planning/internal/application/usecases"
)

type stationServer struct {
	*httptest.Server
	tally *memory.StorageTallyRepo
}

// newStationServer wires the whole REST surface, station capacity included,
// over in-memory adapters, keeping the tally so tests can seed it the way the
// facility consumer would.
func newStationServer(t *testing.T) *stationServer {
	t.Helper()
	pcs, paths, plans, ob := memory.NewProcessCapacityRepo(), memory.NewProcessPathRepo(), memory.NewCapacityPlanRepo(), memory.NewOutboxRepo()
	standards, tallyRepo := memory.NewStationStandardRepo(), memory.NewStorageTallyRepo()
	uow := memory.NewUnitOfWork(pcs, plans, ob)
	enc := outboundkafka.NewEncoder()
	pathCapacity := &usecases.GetProcessPathCapacity{ProcessPaths: paths, ProcessCapacities: pcs, StationStandards: standards, Tally: tallyRepo}
	srv := httptest.NewServer(inboundhttp.NewRouter(&inboundhttp.Server{
		RegisterProcessCapacityConstraint: &usecases.RegisterProcessCapacityConstraint{Repo: pcs},
		ProcessCapacities:                 pcs,
		RegisterProcessPath:               &usecases.RegisterProcessPath{Repo: paths},
		GetProcessPathCapacity:            pathCapacity,
		CreateCapacityPlan:                &usecases.CreateCapacityPlan{PathCapacity: pathCapacity, Plans: plans, Outbox: ob, Encoder: enc, UnitOfWork: uow},
		PublishCapacityPlan:               &usecases.PublishCapacityPlan{Plans: plans, Outbox: ob, Encoder: enc, UnitOfWork: uow},
		CapacityPlans:                     plans,
		DeclareStationStandard:            &usecases.DeclareStationStandard{Repo: standards},
		StationStandards:                  standards,
		GetStorageCapacity:                &usecases.GetStorageCapacity{Tally: tallyRepo},
	}))
	t.Cleanup(srv.Close)
	return &stationServer{Server: srv, tally: tallyRepo}
}

func (s *stationServer) seed(t *testing.T, zone, tallyType, key string, n int) {
	t.Helper()
	for i := 0; i < n; i++ {
		code := zone + "-" + key + "-" + strconv.Itoa(i)
		if _, err := s.tally.RegisterSlot(context.Background(), code, zone, tallyType, []string{key}); err != nil {
			t.Fatal(err)
		}
	}
}

func putJSON(t *testing.T, srv *httptest.Server, path string, payload any) (int, http.Header, []byte) {
	t.Helper()
	encoded, err := json.Marshal(payload)
	if err != nil {
		t.Fatal(err)
	}
	return putRaw(t, srv, path, encoded)
}

func putRaw(t *testing.T, srv *httptest.Server, path string, body []byte) (int, http.Header, []byte) {
	t.Helper()
	req, err := http.NewRequest(http.MethodPut, srv.URL+path, bytes.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Content-Type", "application/json")
	return doRequest(t, req)
}

func standardBody(qty float64, unit string) map[string]any {
	return map[string]any{"quantity": qty, "unit": unit, "period_seconds": 3600}
}

func decodeMap(t *testing.T, body []byte) map[string]any {
	t.Helper()
	var m map[string]any
	if err := json.Unmarshal(body, &m); err != nil {
		t.Fatalf("decode: %v (%s)", err, body)
	}
	return m
}

func TestHandler_PutStationStandard_CreatesThenReplaces(t *testing.T) {
	srv := newStationServer(t)

	status, _, body := putJSON(t, srv.Server, "/station-standards/SIM1/PACK", standardBody(180, "PACKAGE"))
	want := map[string]any{"location": "SIM1", "process_type": "PACK", "quantity": 180.0, "unit": "PACKAGE", "period_seconds": 3600.0}
	if status != http.StatusCreated || !reflect.DeepEqual(decodeMap(t, body), want) {
		t.Fatalf("first PUT = %d %s, want 201 %v", status, body, want)
	}

	status, _, body = putJSON(t, srv.Server, "/station-standards/SIM1/PACK", standardBody(200.5, "PACKAGE"))
	if status != http.StatusOK || decodeMap(t, body)["quantity"] != 200.5 {
		t.Fatalf("second PUT = %d %s, want 200 with 200.5", status, body)
	}
}

func TestHandler_PutStationStandard_Rejections(t *testing.T) {
	srv := newStationServer(t)
	tests := []struct {
		name string
		body map[string]any
		slug string
	}{
		{"zero quantity", standardBody(0, "PACKAGE"), "non-positive-station-standard"},
		{"negative quantity", standardBody(-1, "PACKAGE"), "negative-quantity"},
		{"LINE unit", standardBody(180, "LINE"), "unsupported-normalization-unit"},
		{"missing unit", map[string]any{"quantity": 180, "period_seconds": 3600}, "unsupported-normalization-unit"},
		{"zero period", map[string]any{"quantity": 180, "unit": "PACKAGE", "period_seconds": 0}, "non-positive-period"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			status, hdr, body := putJSON(t, srv.Server, "/station-standards/SIM1/PACK", tc.body)
			if status != http.StatusUnprocessableEntity || hdr.Get("Content-Type") != "application/problem+json" || problemType(t, body) != tc.slug {
				t.Fatalf("PUT = %d %s %s, want 422 problem+json %s", status, hdr.Get("Content-Type"), body, tc.slug)
			}
		})
	}
	status, _, body := putRaw(t, srv.Server, "/station-standards/SIM1/PACK", []byte("{not json"))
	if status != http.StatusBadRequest || problemType(t, body) != "malformed-json" {
		t.Fatalf("malformed JSON = %d %s, want 400 malformed-json", status, body)
	}
	if status, _, body := getPath(t, srv.Server, "/station-standards"); status != http.StatusOK || len(decodeMap(t, body)["standards"].([]any)) != 0 {
		t.Fatalf("a rejected declaration was stored: %d %s", status, body)
	}
}

func TestHandler_ListStationStandards(t *testing.T) {
	srv := newStationServer(t)
	for _, d := range []struct {
		path string
		qty  float64
		unit string
	}{{"/station-standards/SIM2/PACK", 75, "ORDER"}, {"/station-standards/SIM1/SORT", 90, "UNIT"}, {"/station-standards/SIM1/PACK", 180, "PACKAGE"}} {
		if status, _, body := putJSON(t, srv.Server, d.path, standardBody(d.qty, d.unit)); status != http.StatusCreated {
			t.Fatalf("PUT %s = %d %s", d.path, status, body)
		}
	}

	status, _, body := getPath(t, srv.Server, "/station-standards?location=SIM1")
	got := decodeMap(t, body)
	standards := got["standards"].([]any)
	if status != http.StatusOK || got["location"] != "SIM1" || len(standards) != 2 ||
		standards[0].(map[string]any)["process_type"] != "PACK" || standards[1].(map[string]any)["process_type"] != "SORT" {
		t.Fatalf("GET ?location=SIM1 = %d %s, want PACK then SORT", status, body)
	}

	_, _, body = getPath(t, srv.Server, "/station-standards")
	if got := decodeMap(t, body); len(got["standards"].([]any)) != 3 || got["location"] != nil {
		t.Fatalf("GET all = %s, want 3 standards and no location", body)
	}
	_, _, body = getPath(t, srv.Server, "/station-standards?location=NOWHERE")
	if got := decodeMap(t, body); !reflect.DeepEqual(got["standards"], []any{}) {
		t.Fatalf("GET NOWHERE = %s, want standards: []", body)
	}
}

func TestHandler_GetStorageCapacity(t *testing.T) {
	srv := newStationServer(t)

	status, _, body := getPath(t, srv.Server, "/storage-capacity?location=SIM1")
	got := decodeMap(t, body)
	if status != http.StatusOK || got["location"] != "SIM1" || !reflect.DeepEqual(got["storage_positions"], []any{}) || !reflect.DeepEqual(got["stations"], []any{}) {
		t.Fatalf("empty site = %d %s, want 200 with empty lists", status, body)
	}

	srv.seed(t, "SIM1-OPS-WC", tally.TypeStation, "PACK", 11)
	srv.seed(t, "SIM1-STOR-AMB", tally.TypeLocation, "SimShelf", 24)
	srv.seed(t, "SIM2-OPS-WC", tally.TypeStation, "PACK", 4)
	_, _, body = getPath(t, srv.Server, "/storage-capacity?location=SIM1")
	got = decodeMap(t, body)
	wantStations := []any{map[string]any{"zone_id": "SIM1-OPS-WC", "activity": "PACK", "stations": 11.0}}
	wantPositions := []any{map[string]any{"zone_id": "SIM1-STOR-AMB", "location_type": "SimShelf", "positions": 24.0}}
	if !reflect.DeepEqual(got["stations"], wantStations) || !reflect.DeepEqual(got["storage_positions"], wantPositions) {
		t.Fatalf("SIM1 = %s, want stations %v positions %v", body, wantStations, wantPositions)
	}

	status, hdr, body := getPath(t, srv.Server, "/storage-capacity")
	if status != http.StatusBadRequest || hdr.Get("Content-Type") != "application/problem+json" || problemType(t, body) != "missing-location" {
		t.Fatalf("no location = %d %s, want 400 missing-location", status, body)
	}
}

// stepItem is one expected step_breakdown entry.
func stepItem(rate float64, step, binding string) any {
	return map[string]any{"step": step, "normalized_rate": rate, "binding_constraint": binding}
}

// expectPath asserts a path capacity body: rate/unit/bottleneck, the exact
// step_breakdown and the number of warnings (as a JSON array, never null).
func expectPath(t *testing.T, got map[string]any, rate float64, bottleneck string, breakdown []any, warnings int) {
	t.Helper()
	w, isArray := got["warnings"].([]any)
	if got["normalized_rate"] != rate || got["normalized_unit"] != "ORDER" || got["bottleneck_step"] != bottleneck ||
		!reflect.DeepEqual(got["step_breakdown"], breakdown) || !isArray || len(w) != warnings {
		t.Fatalf("path capacity = %v, want %v ORDER bound by %s, breakdown %v, %d warnings", got, rate, bottleneck, breakdown, warnings)
	}
}

func (s *stationServer) seedFixtureB(t *testing.T) {
	t.Helper()
	for _, reg := range []struct {
		process string
		qty     float64
		unit    string
	}{{"PICK", 8000, "UNIT"}, {"REBIN", 2500, "UNIT"}, {"PACK", 2500, "PACKAGE"}} {
		s.setLabor(t, reg.process, reg.qty, reg.unit)
	}
	registerProcessPath(t, s.Server, "pick-rebin-pack", "Pick-Rebin-Pack", []string{"PICK", "REBIN", "PACK"})
	s.seed(t, "SIM1-OPS-WC", tally.TypeStation, "PACK", 10)
}

func (s *stationServer) setLabor(t *testing.T, process string, qty float64, unit string) {
	t.Helper()
	if status, _, body := postJSON(t, s.Server, "/process-capacities", map[string]any{
		"process_type": process, "location": "SIM1", "window_start": planStart, "window_end": planEnd,
		"constraint_type": "LABOR", "quantity": qty, "unit": unit, "period_seconds": 3600,
	}); status != http.StatusCreated {
		t.Fatalf("register %s: %d %s", process, status, body)
	}
}

func (s *stationServer) pathCapacity(t *testing.T) map[string]any {
	t.Helper()
	upo, ppo := 2.5, 1.0
	status, _, body := getProcessPathCapacity(t, s.Server, "pick-rebin-pack", "SIM1", planStart, planEnd, &upo, &ppo)
	if status != http.StatusOK {
		t.Fatalf("path capacity = %d %s", status, body)
	}
	return decodeMap(t, body)
}

// FIXTURE C over REST: stations tallied, no standard -> labor only + a warning.
func TestHandler_StationComposition_FixtureC_NoStandardWarns(t *testing.T) {
	srv := newStationServer(t)
	srv.seedFixtureB(t)
	expectPath(t, srv.pathCapacity(t), 1000, "REBIN",
		[]any{stepItem(3200, "PICK", "LABOR"), stepItem(1000, "REBIN", "LABOR"), stepItem(2500, "PACK", "LABOR")}, 1)
}

// FIXTURES A+B over REST: path capacity reports step_breakdown (3200/1000/1800
// then 3200/2400/1800) and a plan carries the binding constraint -- also when
// read back with GET.
func TestHandler_StationComposition_FixtureB_BreakdownAndPlan(t *testing.T) {
	srv := newStationServer(t)
	srv.seedFixtureB(t)
	putJSON(t, srv.Server, "/station-standards/SIM1/PACK", standardBody(180, "PACKAGE"))

	expectPath(t, srv.pathCapacity(t), 1000, "REBIN",
		[]any{stepItem(3200, "PICK", "LABOR"), stepItem(1000, "REBIN", "LABOR"), stepItem(1800, "PACK", "STATION")}, 0)

	srv.setLabor(t, "REBIN", 6000, "UNIT")
	expectPath(t, srv.pathCapacity(t), 1800, "PACK",
		[]any{stepItem(3200, "PICK", "LABOR"), stepItem(2400, "REBIN", "LABOR"), stepItem(1800, "PACK", "STATION")}, 0)

	body := planBody(20000)
	body["location"] = "SIM1"
	status, _, raw := postJSON(t, srv.Server, "/capacity-plans", body)
	plan := decodeMap(t, raw)
	if status != http.StatusCreated || plan["path_capacity"] != 1800.0 || plan["bottleneck_step"] != "PACK" ||
		plan["bottleneck_constraint"] != "STATION" || plan["shortage"] != 5600.0 || !reflect.DeepEqual(plan["warnings"], []any{}) {
		t.Fatalf("plan = %d %s", status, raw)
	}
	_, _, raw = getPath(t, srv.Server, "/capacity-plans/"+plan["id"].(string))
	if got := decodeMap(t, raw); got["bottleneck_constraint"] != "STATION" {
		t.Fatalf("GET plan = %s, want the stored bottleneck_constraint STATION", raw)
	}
}

// The CORS preflight for PUT is allowed (browsers' operator UIs).
func TestRouter_CORSAllowsPut(t *testing.T) {
	srv := newStationServer(t)
	req, _ := http.NewRequest(http.MethodOptions, srv.URL+"/station-standards/SIM1/PACK", nil)
	req.Header.Set("Origin", "http://localhost:5173")
	req.Header.Set("Access-Control-Request-Method", http.MethodPut)
	_, hdr, _ := doRequest(t, req)
	if got := hdr.Get("Access-Control-Allow-Methods"); got != http.MethodPut {
		t.Fatalf("Access-Control-Allow-Methods = %q, want PUT", got)
	}
}
