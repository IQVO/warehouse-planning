package http_test

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	inboundhttp "github.com/claudioed/warehouse-planning/internal/adapters/inbound/http"
	outboundkafka "github.com/claudioed/warehouse-planning/internal/adapters/outbound/kafka"
	"github.com/claudioed/warehouse-planning/internal/adapters/outbound/memory"
	"github.com/claudioed/warehouse-planning/internal/application/usecases"
	"github.com/claudioed/warehouse-planning/internal/domain/capacityplan"
)

const (
	planStart = "2026-10-05T08:00:00Z"
	planEnd   = "2026-10-05T16:00:00Z"
)

type planServer struct {
	*httptest.Server
	outbox *memory.OutboxRepo
}

// newPlanServer wires the full Phase 4 surface over in-memory adapters,
// including the real CloudEvents encoder.
func newPlanServer(t *testing.T) *planServer {
	t.Helper()
	pcs, paths, plans, ob := memory.NewProcessCapacityRepo(), memory.NewProcessPathRepo(), memory.NewCapacityPlanRepo(), memory.NewOutboxRepo()
	uow := memory.NewUnitOfWork(pcs, plans, ob)
	enc := outboundkafka.NewEncoder()
	s := &inboundhttp.Server{
		RegisterProcessCapacityConstraint: &usecases.RegisterProcessCapacityConstraint{Repo: pcs},
		ProcessCapacities:                 pcs,
		RegisterProcessPath:               &usecases.RegisterProcessPath{Repo: paths},
		GetProcessPathCapacity:            &usecases.GetProcessPathCapacity{ProcessPaths: paths, ProcessCapacities: pcs, StationStandards: memory.NewStationStandardRepo(), Tally: memory.NewStorageTallyRepo()},
		CreateCapacityPlan: &usecases.CreateCapacityPlan{
			PathCapacity: &usecases.GetProcessPathCapacity{ProcessPaths: paths, ProcessCapacities: pcs, StationStandards: memory.NewStationStandardRepo(), Tally: memory.NewStorageTallyRepo()},
			Plans:        plans, Outbox: ob, Encoder: enc, UnitOfWork: uow,
		},
		PublishCapacityPlan: &usecases.PublishCapacityPlan{Plans: plans, Outbox: ob, Encoder: enc, UnitOfWork: uow},
		CapacityPlans:       plans,
	}
	srv := httptest.NewServer(inboundhttp.NewRouter(s))
	t.Cleanup(srv.Close)
	return &planServer{Server: srv, outbox: ob}
}

// seedPlanFixture registers PICK/REBIN/PACK capacities for the 8h plan
// window at PATH-ZONE-A plus the pick-rebin-pack path.
func seedPlanFixture(t *testing.T, srv *httptest.Server) {
	t.Helper()
	for _, reg := range []struct {
		process string
		qty     float64
		unit    string
	}{{"PICK", 4000, "UNIT"}, {"REBIN", 2500, "UNIT"}, {"PACK", 1800, "PACKAGE"}} {
		status, _, body := postJSON(t, srv, "/process-capacities", map[string]any{
			"process_type": reg.process, "location": "PATH-ZONE-A",
			"window_start": planStart, "window_end": planEnd,
			"constraint_type": "LABOR", "quantity": reg.qty, "unit": reg.unit, "period_seconds": 3600,
		})
		if status != http.StatusCreated {
			t.Fatalf("register %s: %d %s", reg.process, status, body)
		}
	}
	if status, _, body := registerProcessPath(t, srv, "pick-rebin-pack", "Pick-Rebin-Pack", []string{"PICK", "REBIN", "PACK"}); status != http.StatusCreated {
		t.Fatalf("register path: %d %s", status, body)
	}
}

func planBody(demand any) map[string]any {
	b := map[string]any{
		"warehouse_id": "WH-1", "site_id": "SIM1", "location": "PATH-ZONE-A",
		"window_start": planStart, "window_end": planEnd,
		"path_id": "pick-rebin-pack", "units_per_order": 2.5, "packages_per_order": 1,
	}
	if demand != nil {
		b["assigned_demand"] = demand
	}
	return b
}

type planJSON struct {
	ID                 string  `json:"id"`
	WarehouseID        string  `json:"warehouse_id"`
	SiteID             string  `json:"site_id"`
	Location           string  `json:"location"`
	WindowStart        string  `json:"window_start"`
	WindowEnd          string  `json:"window_end"`
	PathID             string  `json:"path_id"`
	AssignedDemand     float64 `json:"assigned_demand"`
	Status             string  `json:"status"`
	PathCapacity       float64 `json:"path_capacity"`
	BottleneckStep     string  `json:"bottleneck_step"`
	CapacityOverWindow float64 `json:"capacity_over_window"`
	Shortage           float64 `json:"shortage"`
	CreatedAt          string  `json:"created_at"`
	PublishedAt        *string `json:"published_at"`
}

func decodePlan(t *testing.T, body []byte) planJSON {
	t.Helper()
	var p planJSON
	if err := json.Unmarshal(body, &p); err != nil {
		t.Fatalf("decode plan: %v (%s)", err, body)
	}
	return p
}

func problemType(t *testing.T, body []byte) string {
	t.Helper()
	var p struct {
		Type   string `json:"type"`
		Status int    `json:"status"`
	}
	if err := json.Unmarshal(body, &p); err != nil {
		t.Fatalf("decode problem: %v (%s)", err, body)
	}
	return p.Type[len("https://errors.warehouse-planning.warehouse-systems.dev/"):]
}

func TestHandler_CapacityPlan_WorkedExample(t *testing.T) {
	srv := newPlanServer(t)
	seedPlanFixture(t, srv.Server)

	status, headers, body := postJSON(t, srv.Server, "/capacity-plans", planBody(12000))
	if status != http.StatusCreated {
		t.Fatalf("create = %d: %s", status, body)
	}
	if ct := headers.Get("Content-Type"); ct != "application/json" {
		t.Errorf("Content-Type = %q", ct)
	}
	plan := decodePlan(t, body)
	assertWorkedExamplePlan(t, plan, "DRAFT")
	if plan.PublishedAt != nil {
		t.Errorf("DRAFT plan has published_at %v", *plan.PublishedAt)
	}

	// GET returns the same plan.
	status, _, body = getPath(t, srv.Server, "/capacity-plans/"+plan.ID)
	if status != http.StatusOK {
		t.Fatalf("get = %d: %s", status, body)
	}
	if fetched := decodePlan(t, body); fetched != plan {
		t.Errorf("GET = %+v, want %+v", fetched, plan)
	}

	// Publish -> 200 PUBLISHED with published_at; the outbox then holds all four events.
	status, _, body = postJSON(t, srv.Server, "/capacity-plans/"+plan.ID+"/publish", nil)
	if status != http.StatusOK {
		t.Fatalf("publish = %d: %s", status, body)
	}
	published := decodePlan(t, body)
	assertWorkedExamplePlan(t, published, "PUBLISHED")
	if published.PublishedAt == nil {
		t.Error("published plan has no published_at")
	}
	assertOutboxTypes(t, srv.outbox, "CapacityPlanCreated", "CapacityPlanPublished", "CapacityShortageDetected", "BottleneckDetected")

	// Second publish -> 409 and nothing new queued.
	status, headers, body = postJSON(t, srv.Server, "/capacity-plans/"+plan.ID+"/publish", nil)
	if status != http.StatusConflict || headers.Get("Content-Type") != "application/problem+json" || problemType(t, body) != "capacity-plan-already-published" {
		t.Errorf("second publish = %d %s %s", status, headers.Get("Content-Type"), body)
	}
	assertOutboxTypes(t, srv.outbox, "CapacityPlanCreated", "CapacityPlanPublished", "CapacityShortageDetected", "BottleneckDetected")

	// GET after publish reflects the new state.
	_, _, body = getPath(t, srv.Server, "/capacity-plans/"+plan.ID)
	if decodePlan(t, body).Status != "PUBLISHED" {
		t.Errorf("GET after publish = %s", body)
	}
}

// assertWorkedExamplePlan checks every field of the section-43 plan
// (12000 orders, 8h, 1000 ORDER/h, REBIN) except the generated ones.
func assertWorkedExamplePlan(t *testing.T, plan planJSON, status string) {
	t.Helper()
	want := planJSON{
		WarehouseID: "WH-1", SiteID: "SIM1", Location: "PATH-ZONE-A", WindowStart: planStart, WindowEnd: planEnd, PathID: "pick-rebin-pack",
		AssignedDemand: 12000, Status: status, PathCapacity: 1000, BottleneckStep: "REBIN", CapacityOverWindow: 8000, Shortage: 4000,
	}
	if plan.ID == "" || plan.CreatedAt == "" {
		t.Errorf("id/created_at = %q/%q", plan.ID, plan.CreatedAt)
	}
	got := plan
	got.ID, got.CreatedAt, got.PublishedAt = "", "", nil
	if got != want {
		t.Errorf("plan = %+v\nwant %+v", got, want)
	}
}

func assertOutboxTypes(t *testing.T, ob *memory.OutboxRepo, names ...string) {
	t.Helper()
	var got, want []string
	for _, m := range ob.Messages() {
		got = append(got, m.EventType)
	}
	for _, n := range names {
		want = append(want, "com.warehouse.wes.warehouse-planning.capacityplan."+n)
	}
	if strings.Join(got, "|") != strings.Join(want, "|") {
		t.Errorf("outbox types = %v, want %v", got, want)
	}
}

func TestHandler_CapacityPlan_WithinCapacityHasNoShortage(t *testing.T) {
	srv := newPlanServer(t)
	seedPlanFixture(t, srv.Server)
	status, _, body := postJSON(t, srv.Server, "/capacity-plans", planBody(6000))
	if status != http.StatusCreated {
		t.Fatalf("create = %d: %s", status, body)
	}
	if p := decodePlan(t, body); p.Shortage != 0 || p.CapacityOverWindow != 8000 {
		t.Errorf("plan = %+v", p)
	}
}

func TestHandler_CapacityPlan_CreateRejections(t *testing.T) {
	cases := []struct {
		name       string
		mutate     func(map[string]any)
		wantStatus int
		wantType   string
	}{
		{"unknown path", func(b map[string]any) { b["path_id"] = "nope" }, http.StatusNotFound, "process-path-not-found"},
		{"step without capacity (other location)", func(b map[string]any) { b["location"] = "ELSEWHERE" }, http.StatusUnprocessableEntity, "missing-step-capacity"},
		{"step without capacity (other window)", func(b map[string]any) { b["window_end"] = "2026-10-05T17:00:00Z" }, http.StatusUnprocessableEntity, "missing-step-capacity"},
		{"non-positive factor", func(b map[string]any) { b["units_per_order"] = 0 }, http.StatusUnprocessableEntity, "non-positive-conversion-factor"},
		{"missing factor", func(b map[string]any) { delete(b, "units_per_order") }, http.StatusUnprocessableEntity, "missing-conversion-factor"},
		{"negative demand", func(b map[string]any) { b["assigned_demand"] = -1 }, http.StatusUnprocessableEntity, "negative-assigned-demand"},
		{"absent demand", func(b map[string]any) { delete(b, "assigned_demand") }, http.StatusUnprocessableEntity, "missing-assigned-demand"},
		{"blank warehouse", func(b map[string]any) { b["warehouse_id"] = "" }, http.StatusUnprocessableEntity, "missing-required-field"},
		{"blank site id", func(b map[string]any) { b["site_id"] = "" }, http.StatusUnprocessableEntity, "missing-required-field"},
		{"absent site id", func(b map[string]any) { delete(b, "site_id") }, http.StatusUnprocessableEntity, "missing-required-field"},
		{"inverted window", func(b map[string]any) { b["window_end"] = planStart }, http.StatusBadRequest, "invalid-capacity-window"},
		{"malformed window_start", func(b map[string]any) { b["window_start"] = "yesterday" }, http.StatusBadRequest, "malformed-window-start"},
		{"malformed window_end", func(b map[string]any) { b["window_end"] = "tomorrow" }, http.StatusBadRequest, "malformed-window-end"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			srv := newPlanServer(t)
			seedPlanFixture(t, srv.Server)
			body := planBody(12000)
			tc.mutate(body)
			status, headers, resp := postJSON(t, srv.Server, "/capacity-plans", body)
			if status != tc.wantStatus {
				t.Fatalf("status = %d, want %d: %s", status, tc.wantStatus, resp)
			}
			if headers.Get("Content-Type") != "application/problem+json" || problemType(t, resp) != tc.wantType {
				t.Errorf("content-type %q type %q, want problem+json / %s", headers.Get("Content-Type"), problemType(t, resp), tc.wantType)
			}
			if n := len(srv.outbox.Messages()); n != 0 {
				t.Errorf("%d outbox rows written for a rejected request", n)
			}
		})
	}
}

func TestHandler_CapacityPlan_MalformedJSONIs400(t *testing.T) {
	srv := newPlanServer(t)
	resp, err := http.Post(srv.URL+"/capacity-plans", "application/json", http.NoBody)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400", resp.StatusCode)
	}
}

func TestHandler_CapacityPlan_UnknownIDIs404(t *testing.T) {
	srv := newPlanServer(t)
	for _, c := range []struct {
		name   string
		status func() (int, []byte)
	}{
		{"get", func() (int, []byte) { s, _, b := getPath(t, srv.Server, "/capacity-plans/ghost"); return s, b }},
		{"publish", func() (int, []byte) {
			s, _, b := postJSON(t, srv.Server, "/capacity-plans/ghost/publish", nil)
			return s, b
		}},
	} {
		status, body := c.status()
		if status != http.StatusNotFound || problemType(t, body) != "capacity-plan-not-found" {
			t.Errorf("%s ghost = %d %s", c.name, status, body)
		}
	}
}

type brokenPlans struct{}

func (brokenPlans) Save(context.Context, *capacityplan.CapacityPlan) error {
	return errors.New("db down")
}
func (brokenPlans) FindByID(context.Context, string) (*capacityplan.CapacityPlan, error) {
	return nil, errors.New("db down")
}

// An infrastructure failure is a 500 problem+json, never a 404/409.
func TestHandler_CapacityPlan_StorageFailureIs500(t *testing.T) {
	uow := memory.NewUnitOfWork()
	s := &inboundhttp.Server{
		PublishCapacityPlan: &usecases.PublishCapacityPlan{Plans: brokenPlans{}, UnitOfWork: uow},
		CapacityPlans:       brokenPlans{},
	}
	srv := httptest.NewServer(inboundhttp.NewRouter(s))
	defer srv.Close()

	status, headers, body := getPath(t, srv, "/capacity-plans/x")
	if status != http.StatusInternalServerError || headers.Get("Content-Type") != "application/problem+json" || problemType(t, body) != "internal-error" {
		t.Errorf("get = %d %s", status, body)
	}
	status, _, body = postJSON(t, srv, "/capacity-plans/x/publish", nil)
	if status != http.StatusInternalServerError || problemType(t, body) != "internal-error" {
		t.Errorf("publish = %d %s", status, body)
	}
}
