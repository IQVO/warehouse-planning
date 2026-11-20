package http_test

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	inboundhttp "github.com/claudioed/warehouse-planning/internal/adapters/inbound/http"
	"github.com/claudioed/warehouse-planning/internal/adapters/outbound/memory"
	"github.com/claudioed/warehouse-planning/internal/application/usecases"
	"github.com/claudioed/warehouse-planning/internal/domain/processflow"
)

// newFlowPathServer wires the path reads the way cmd/api does (docs/adr/0025
// stage 3): the flow finder and lister over the flows copy.
func newFlowPathServer(t *testing.T) (*httptest.Server, *memory.ProcessFlowRepo) {
	t.Helper()
	capRepo, flows := memory.NewProcessCapacityRepo(), memory.NewProcessFlowRepo()
	finder := &usecases.FlowPathFinder{Flows: flows}
	s := &inboundhttp.Server{
		RegisterProcessCapacityConstraint: &usecases.RegisterProcessCapacityConstraint{Repo: capRepo},
		ProcessCapacities:                 capRepo,
		GetProcessPathCapacity: &usecases.GetProcessPathCapacity{
			ProcessPaths: finder, ProcessCapacities: capRepo, StationStandards: memory.NewStationStandardRepo(), Tally: memory.NewStorageTallyRepo(),
		},
		ListProcessPaths: &usecases.ListProcessPaths{Paths: &usecases.FlowPathLister{Flows: flows}},
		GetProcessPath:   &usecases.GetProcessPath{Paths: finder},
	}
	srv := httptest.NewServer(inboundhttp.NewRouter(s))
	t.Cleanup(srv.Close)
	return srv, flows
}

func putFlow(t *testing.T, flows *memory.ProcessFlowRepo, id string, version int64, status processflow.Status, types ...processflow.ProcessType) {
	t.Helper()
	stages := make([]processflow.Stage, 0, len(types))
	for _, pt := range types {
		stages = append(stages, processflow.Stage{PathID: string(pt), ProcessType: pt})
	}
	f, err := processflow.New(processflow.Params{
		FlowID: id, Name: "Flow " + id, Stages: stages, Status: status, Version: version,
		UpdatedAt: time.Date(2026, 11, 20, 8, 0, 0, 0, time.UTC),
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := flows.Record(context.Background(), f); err != nil {
		t.Fatal(err)
	}
}

type pathView struct {
	ID     string   `json:"id"`
	Name   string   `json:"name"`
	Steps  []string `json:"steps"`
	Source string   `json:"source"`
}

func decodePath(t *testing.T, body []byte) pathView {
	t.Helper()
	var p pathView
	if err := json.Unmarshal(body, &p); err != nil {
		t.Fatalf("decode %s: %v", body, err)
	}
	return p
}

func seedFlows(t *testing.T, srv *httptest.Server, flows *memory.ProcessFlowRepo) {
	t.Helper()
	putFlow(t, flows, "both", 1, processflow.StatusActive, processflow.Pick, processflow.Rebin, processflow.Pack)
	putFlow(t, flows, "flow-only", 1, processflow.StatusActive, processflow.Dispatch)
	putFlow(t, flows, "gone", 2, processflow.StatusRetired, processflow.Pick)
}

func requirePathByID(t *testing.T, srv *httptest.Server, id string, want pathView) {
	t.Helper()
	status, _, body := getPath(t, srv, "/process-paths/"+id)
	got := decodePath(t, body)
	if status != http.StatusOK || got.ID != want.ID || got.Name != want.Name || got.Source != want.Source || len(got.Steps) != len(want.Steps) {
		t.Fatalf("GET %s = %d %+v, want %+v", id, status, got, want)
	}
}

func requireListedPaths(t *testing.T, srv *httptest.Server, want []string) {
	t.Helper()
	status, _, body := getPath(t, srv, "/process-paths?limit=10")
	if status != http.StatusOK {
		t.Fatalf("list: %d %s", status, body)
	}
	var page struct {
		Items []pathView `json:"items"`
	}
	if err := json.Unmarshal(body, &page); err != nil {
		t.Fatal(err)
	}
	got := make([]string, 0, len(page.Items))
	for _, it := range page.Items {
		got = append(got, it.ID+":"+it.Source)
	}
	if strings.Join(got, ",") != strings.Join(want, ",") {
		t.Fatalf("list = %v, want %v", got, want)
	}
}

func TestHandler_ProcessPaths_SourceOnEveryRead(t *testing.T) {
	srv, flows := newFlowPathServer(t)
	seedFlows(t, srv, flows)

	// By id: an ACTIVE flow is a 200 with source flow.
	requirePathByID(t, srv, "both", pathView{ID: "both", Name: "Flow both", Steps: []string{"PICK", "REBIN", "PACK"}, Source: "flow"})
	requirePathByID(t, srv, "flow-only", pathView{ID: "flow-only", Name: "Flow flow-only", Steps: []string{"DISPATCH"}, Source: "flow"})
	// Unknown, or only a RETIRED flow: 404 problem.
	for _, id := range []string{"nowhere", "gone"} {
		if status, _, body := getPath(t, srv, "/process-paths/"+id); status != http.StatusNotFound {
			t.Fatalf("GET %s = %d %s, want 404", id, status, body)
		}
	}
	// The list: the ACTIVE flows ordered by id; RETIRED never shows.
	requireListedPaths(t, srv, []string{"both:flow", "flow-only:flow"})
}

func TestHandler_ProcessPathCapacity_ReportsTheSourceOfThePath(t *testing.T) {
	srv, flows := newFlowPathServer(t)
	registerPickRebinPackCapacities(t, srv, "PATH-ZONE-A")
	putFlow(t, flows, "pick-rebin-pack", 1, processflow.StatusActive, processflow.Pick, processflow.Rebin, processflow.Pack)
	units, packages := 2.5, 1.0
	source := func() string {
		status, _, body := getProcessPathCapacity(t, srv, "pick-rebin-pack", "PATH-ZONE-A", "2026-10-05T08:00:00Z", "2026-10-05T09:00:00Z", &units, &packages)
		if status != http.StatusOK {
			t.Fatalf("capacity: %d %s", status, body)
		}
		var out struct {
			Source string `json:"source"`
		}
		if err := json.Unmarshal(body, &out); err != nil {
			t.Fatal(err)
		}
		return out.Source
	}
	if got := source(); got != "flow" {
		t.Fatalf("source = %q, want flow", got)
	}
	putFlow(t, flows, "pick-rebin-pack", 2, processflow.StatusActive, processflow.Pick, processflow.Pack)
	if got := source(); got != "flow" {
		t.Fatalf("source = %q, want flow after a revision", got)
	}
	putFlow(t, flows, "pick-rebin-pack", 3, processflow.StatusRetired, processflow.Pick, processflow.Pack)
	if status, _, body := getProcessPathCapacity(t, srv, "pick-rebin-pack", "PATH-ZONE-A", "2026-10-05T08:00:00Z", "2026-10-05T09:00:00Z", &units, &packages); status != http.StatusNotFound {
		t.Fatalf("after retirement = %d %s, want 404", status, body)
	}
}

// A Server built without the GetProcessPath use case behaves exactly as before:
// the by-id route is not registered.
func TestHandler_GetProcessPathRouteIsAbsentWithoutTheUseCase(t *testing.T) {
	srv, _, _ := newTestServer()
	defer srv.Close()
	if status, _, _ := getPath(t, srv, "/process-paths/anything"); status != http.StatusNotFound && status != http.StatusMethodNotAllowed {
		t.Fatalf("status = %d, want the router's 404/405", status)
	}
}
