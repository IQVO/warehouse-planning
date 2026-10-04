package http_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	inboundhttp "github.com/claudioed/warehouse-planning/internal/adapters/inbound/http"
	outboundkafka "github.com/claudioed/warehouse-planning/internal/adapters/outbound/kafka"
	"github.com/claudioed/warehouse-planning/internal/adapters/outbound/memory"
	"github.com/claudioed/warehouse-planning/internal/application/usecases"
	"github.com/claudioed/warehouse-planning/internal/domain/capacityplan"
	"github.com/claudioed/warehouse-planning/internal/domain/processpath"
)

// newListServer is newPlanServer plus the two list use cases, with a clock
// that advances one minute per created plan so "newest first" is observable.
func newListServer(t *testing.T) *httptest.Server {
	t.Helper()
	pcs, paths, plans, ob := memory.NewProcessCapacityRepo(), memory.NewProcessPathRepo(), memory.NewCapacityPlanRepo(), memory.NewOutboxRepo()
	uow := memory.NewUnitOfWork(pcs, plans, ob)
	enc := outboundkafka.NewEncoder()
	pathCapacity := &usecases.GetProcessPathCapacity{ProcessPaths: paths, ProcessCapacities: pcs, StationStandards: memory.NewStationStandardRepo(), Tally: memory.NewStorageTallyRepo()}
	clock := time.Date(2026, 10, 4, 9, 0, 0, 0, time.UTC)
	s := &inboundhttp.Server{
		RegisterProcessCapacityConstraint: &usecases.RegisterProcessCapacityConstraint{Repo: pcs},
		ProcessCapacities:                 pcs,
		RegisterProcessPath:               &usecases.RegisterProcessPath{Repo: paths},
		GetProcessPathCapacity:            pathCapacity,
		CreateCapacityPlan: &usecases.CreateCapacityPlan{
			PathCapacity: pathCapacity, Plans: plans, Outbox: ob, Encoder: enc, UnitOfWork: uow,
			Now: func() time.Time { clock = clock.Add(time.Minute); return clock },
		},
		PublishCapacityPlan: &usecases.PublishCapacityPlan{Plans: plans, Outbox: ob, Encoder: enc, UnitOfWork: uow},
		CapacityPlans:       plans,
		ListProcessPaths:    &usecases.ListProcessPaths{Paths: paths},
		ListCapacityPlans:   &usecases.ListCapacityPlans{Plans: plans},
	}
	srv := httptest.NewServer(inboundhttp.NewRouter(s))
	t.Cleanup(srv.Close)
	return srv
}

type plansListJSON struct {
	Location      string                     `json:"location"`
	CapacityPlans []map[string]any           `json:"capacity_plans"`
	Raw           map[string]json.RawMessage `json:"-"`
}

func decodePlansList(t *testing.T, body []byte) plansListJSON {
	t.Helper()
	var out plansListJSON
	if err := json.Unmarshal(body, &out); err != nil {
		t.Fatalf("decode plans list: %v (%s)", err, body)
	}
	if err := json.Unmarshal(body, &out.Raw); err != nil {
		t.Fatal(err)
	}
	return out
}

func planIDs(plans []map[string]any) []string {
	ids := make([]string, 0, len(plans))
	for _, p := range plans {
		ids = append(ids, p["id"].(string))
	}
	return ids
}

func TestHandler_ListProcessPaths(t *testing.T) {
	srv := newListServer(t)

	status, headers, body := getPath(t, srv, "/process-paths")
	if status != http.StatusOK {
		t.Fatalf("empty list = %d: %s", status, body)
	}
	if ct := headers.Get("Content-Type"); ct != "application/json" {
		t.Errorf("Content-Type = %q", ct)
	}
	if string(body) != "{\"process_paths\":[]}\n" {
		t.Errorf("empty body = %q, want an empty array (never null)", body)
	}

	registerProcessPath(t, srv, "pick-rebin-pack", "Pick-Rebin-Pack", []string{"PICK", "REBIN", "PACK"})
	registerProcessPath(t, srv, "a-simple", "Simple", []string{"PACK"})

	status, _, body = getPath(t, srv, "/process-paths")
	if status != http.StatusOK {
		t.Fatalf("list = %d: %s", status, body)
	}
	var got struct {
		ProcessPaths []struct {
			ID    string   `json:"id"`
			Name  string   `json:"name"`
			Steps []string `json:"steps"`
		} `json:"process_paths"`
	}
	if err := json.Unmarshal(body, &got); err != nil {
		t.Fatal(err)
	}
	if len(got.ProcessPaths) != 2 || got.ProcessPaths[0].ID != "a-simple" || got.ProcessPaths[1].ID != "pick-rebin-pack" {
		t.Fatalf("paths = %+v, want ordered by id", got.ProcessPaths)
	}
	if p := got.ProcessPaths[1]; p.Name != "Pick-Rebin-Pack" || fmt.Sprint(p.Steps) != "[PICK REBIN PACK]" {
		t.Errorf("path = %+v, want name and ordered steps", p)
	}
}

func assertListItemsMatchSingleGets(t *testing.T, srv *httptest.Server, items []map[string]any) {
	t.Helper()
	for _, item := range items {
		id := item["id"].(string)
		_, _, single := getPath(t, srv, "/capacity-plans/"+id)
		var want map[string]any
		if err := json.Unmarshal(single, &want); err != nil {
			t.Fatal(err)
		}
		if fmt.Sprint(item) != fmt.Sprint(want) {
			t.Errorf("list item != single GET\n list: %v\n  get: %v", item, want)
		}
	}
}

// seedThreePlans creates three plans at PATH-ZONE-A (demands 1000, 2000, 3000,
// oldest first) and publishes the oldest; it returns their ids oldest first.
func seedThreePlans(t *testing.T, srv *httptest.Server) []string {
	t.Helper()
	seedPlanFixture(t, srv)
	var ids []string
	for i := 0; i < 3; i++ {
		status, _, body := postJSON(t, srv, "/capacity-plans", planBody(float64(1000*(i+1))))
		if status != http.StatusCreated {
			t.Fatalf("create %d = %d: %s", i, status, body)
		}
		ids = append(ids, decodePlan(t, body).ID)
	}
	if status, _, body := postJSON(t, srv, "/capacity-plans/"+ids[0]+"/publish", nil); status != http.StatusOK {
		t.Fatalf("publish = %d: %s", status, body)
	}
	return ids
}

func TestHandler_ListCapacityPlans_NewestFirstInTheSingleGetShape(t *testing.T) {
	srv := newListServer(t)
	ids := seedThreePlans(t, srv)

	status, headers, body := getPath(t, srv, "/capacity-plans?location=PATH-ZONE-A")
	if status != http.StatusOK {
		t.Fatalf("list = %d: %s", status, body)
	}
	if ct := headers.Get("Content-Type"); ct != "application/json" {
		t.Errorf("Content-Type = %q", ct)
	}
	list := decodePlansList(t, body)
	if list.Location != "PATH-ZONE-A" {
		t.Errorf("location echo = %q", list.Location)
	}
	if got := planIDs(list.CapacityPlans); fmt.Sprint(got) != fmt.Sprint([]string{ids[2], ids[1], ids[0]}) {
		t.Fatalf("ids = %v, want newest first %v", got, []string{ids[2], ids[1], ids[0]})
	}

	// Each item is byte-for-byte what GET /capacity-plans/{id} returns.
	assertListItemsMatchSingleGets(t, srv, list.CapacityPlans)
	oldest := list.CapacityPlans[2]
	if oldest["status"] != "PUBLISHED" || oldest["published_at"] == nil {
		t.Errorf("oldest = %v, want PUBLISHED with published_at", oldest)
	}
	if newest := list.CapacityPlans[0]; newest["status"] != "DRAFT" || newest["warnings"] == nil || newest["demand_source"] != "request" {
		t.Errorf("newest = %v, want DRAFT, warnings array, demand_source", newest)
	}
}

func TestHandler_ListCapacityPlans_LimitAndLocationFilters(t *testing.T) {
	srv := newListServer(t)
	ids := seedThreePlans(t, srv)

	_, _, body := getPath(t, srv, "/capacity-plans?location=PATH-ZONE-A&limit=2")
	if got := planIDs(decodePlansList(t, body).CapacityPlans); fmt.Sprint(got) != fmt.Sprint([]string{ids[2], ids[1]}) {
		t.Errorf("limit=2 ids = %v", got)
	}

	// No location filter: every site, and the location field is omitted.
	_, _, body = getPath(t, srv, "/capacity-plans")
	all := decodePlansList(t, body)
	if len(all.CapacityPlans) != 3 {
		t.Errorf("no filter = %d plans, want 3", len(all.CapacityPlans))
	}
	if _, present := all.Raw["location"]; present {
		t.Errorf("location must be omitted when no filter was given: %s", body)
	}

	// An unknown location is an empty list, not an error.
	status, _, body := getPath(t, srv, "/capacity-plans?location=NOWHERE")
	if status != http.StatusOK || string(body) != "{\"location\":\"NOWHERE\",\"capacity_plans\":[]}\n" {
		t.Errorf("unknown location = %d %q", status, body)
	}
}

func TestHandler_ListCapacityPlans_DefaultLimitIs20AndCapIs100(t *testing.T) {
	srv := newListServer(t)
	seedPlanFixture(t, srv)
	for i := 0; i < 105; i++ {
		if status, _, body := postJSON(t, srv, "/capacity-plans", planBody(10)); status != http.StatusCreated {
			t.Fatalf("create %d = %d: %s", i, status, body)
		}
	}
	count := func(query string) int {
		t.Helper()
		status, _, body := getPath(t, srv, "/capacity-plans"+query)
		if status != http.StatusOK {
			t.Fatalf("GET %s = %d: %s", query, status, body)
		}
		return len(decodePlansList(t, body).CapacityPlans)
	}
	if got := count(""); got != 20 {
		t.Errorf("default limit returned %d, want 20", got)
	}
	if got := count("?limit=100"); got != 100 {
		t.Errorf("limit=100 returned %d, want 100", got)
	}
	if got := count("?limit=5000"); got != 100 {
		t.Errorf("limit=5000 returned %d, want the cap of 100", got)
	}
	if got := count("?limit=1"); got != 1 {
		t.Errorf("limit=1 returned %d, want 1", got)
	}
}

func TestHandler_ListCapacityPlans_MalformedLimitIs400Problem(t *testing.T) {
	srv := newListServer(t)
	for _, limit := range []string{"abc", "0", "-5", "1.5", "10x"} {
		status, headers, body := getPath(t, srv, "/capacity-plans?limit="+limit)
		if status != http.StatusBadRequest {
			t.Errorf("limit=%s => %d, want 400", limit, status)
			continue
		}
		if ct := headers.Get("Content-Type"); ct != "application/problem+json" {
			t.Errorf("limit=%s Content-Type = %q", limit, ct)
		}
		if got := problemType(t, body); got != "malformed-limit" {
			t.Errorf("limit=%s problem type = %q", limit, got)
		}
		var p struct{ Title, Detail string }
		if err := json.Unmarshal(body, &p); err != nil || p.Title == "" || p.Detail == "" {
			t.Errorf("limit=%s problem lacks title/detail: %s", limit, body)
		}
	}
}

func TestHandler_ListRoutesAbsentWhenUseCasesNotWired(t *testing.T) {
	srv := newPlanServer(t) // a Server built without the list use cases
	for _, path := range []string{"/process-paths", "/capacity-plans"} {
		if status, _, _ := getPath(t, srv.Server, path); status == http.StatusOK {
			t.Errorf("GET %s answered 200 with the list use case unwired", path)
		}
	}
	// ... and the existing POSTs on the same paths are untouched.
	if status, _, body := postJSON(t, srv.Server, "/process-paths", map[string]any{"id": "x", "name": "x", "steps": []string{"PICK"}}); status != http.StatusCreated {
		t.Errorf("POST /process-paths = %d: %s", status, body)
	}
}

type failingPathLister struct{}

func (failingPathLister) List(context.Context) ([]processpath.ProcessPath, error) {
	return nil, errors.New("database unavailable")
}

type failingPlanLister struct{}

func (failingPlanLister) ListRecent(context.Context, string, int) ([]*capacityplan.CapacityPlan, error) {
	return nil, errors.New("database unavailable")
}

func TestHandler_ListEndpoints_StoreFailureIs500Problem(t *testing.T) {
	s := &inboundhttp.Server{
		ListProcessPaths:  &usecases.ListProcessPaths{Paths: failingPathLister{}},
		ListCapacityPlans: &usecases.ListCapacityPlans{Plans: failingPlanLister{}},
	}
	srv := httptest.NewServer(inboundhttp.NewRouter(s))
	t.Cleanup(srv.Close)
	for _, path := range []string{"/process-paths", "/capacity-plans"} {
		status, headers, body := getPath(t, srv, path)
		if status != http.StatusInternalServerError {
			t.Errorf("GET %s = %d, want 500: %s", path, status, body)
		}
		if ct := headers.Get("Content-Type"); ct != "application/problem+json" {
			t.Errorf("GET %s Content-Type = %q", path, ct)
		}
		if got := problemType(t, body); got != "internal-error" {
			t.Errorf("GET %s problem type = %q", path, got)
		}
	}
}
