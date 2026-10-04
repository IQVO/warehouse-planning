package http_test

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	inboundhttp "github.com/claudioed/warehouse-planning/internal/adapters/inbound/http"
	outboundkafka "github.com/claudioed/warehouse-planning/internal/adapters/outbound/kafka"
	"github.com/claudioed/warehouse-planning/internal/adapters/outbound/memory"
	"github.com/claudioed/warehouse-planning/internal/application/usecases"
	"github.com/claudioed/warehouse-planning/internal/domain/demand"
)

type demandServer struct {
	*httptest.Server
	orders *memory.OrderDemandRepo
}

// newDemandServer wires the whole REST surface over in-memory adapters,
// demand read model included (the way cmd/api does).
func newDemandServer(t *testing.T) *demandServer {
	t.Helper()
	pcs, paths, plans, ob := memory.NewProcessCapacityRepo(), memory.NewProcessPathRepo(), memory.NewCapacityPlanRepo(), memory.NewOutboxRepo()
	orders := memory.NewOrderDemandRepo()
	uow := memory.NewUnitOfWork(pcs, plans, ob)
	enc := outboundkafka.NewEncoder()
	pathCapacity := &usecases.GetProcessPathCapacity{ProcessPaths: paths, ProcessCapacities: pcs, StationStandards: memory.NewStationStandardRepo(), Tally: memory.NewStorageTallyRepo()}
	expected := &usecases.GetExpectedDemand{Demand: orders}
	s := &inboundhttp.Server{
		RegisterProcessCapacityConstraint: &usecases.RegisterProcessCapacityConstraint{Repo: pcs},
		ProcessCapacities:                 pcs,
		RegisterProcessPath:               &usecases.RegisterProcessPath{Repo: paths},
		GetProcessPathCapacity:            pathCapacity,
		CreateCapacityPlan: &usecases.CreateCapacityPlan{
			PathCapacity: pathCapacity, Plans: plans, Outbox: ob, Encoder: enc, UnitOfWork: uow, Demand: expected,
		},
		PublishCapacityPlan: &usecases.PublishCapacityPlan{Plans: plans, Outbox: ob, Encoder: enc, UnitOfWork: uow},
		CapacityPlans:       plans,
		GetExpectedDemand:   expected,
	}
	srv := httptest.NewServer(inboundhttp.NewRouter(s))
	t.Cleanup(srv.Close)
	return &demandServer{Server: srv, orders: orders}
}

var (
	demandStart = time.Date(2026, 10, 5, 8, 0, 0, 0, time.UTC)
	demandEnd   = time.Date(2026, 10, 5, 16, 0, 0, 0, time.UTC)
	demandAsOf  = time.Date(2026, 10, 4, 9, 15, 30, 0, time.UTC)
)

func (s *demandServer) seed(t *testing.T, id, location string, promise time.Time, lines int, asOf time.Time) {
	t.Helper()
	o, err := demand.NewOrder(demand.OrderParams{OrderID: id, Location: location, PromiseAt: promise, ReleasedLines: lines, AsOf: asOf})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.orders.Upsert(context.Background(), o); err != nil {
		t.Fatal(err)
	}
}

func demandQuery(location, start, end string) string {
	return fmt.Sprintf("/demand?location=%s&window_start=%s&window_end=%s", location, start, end)
}

func TestGetDemand_CountsTheHalfOpenWindowAndReportsFreshness(t *testing.T) {
	srv := newDemandServer(t)
	srv.seed(t, "at-start", "SIM1", demandStart, 2, demandAsOf)
	srv.seed(t, "last-second", "SIM1", demandEnd.Add(-time.Second), 3, demandAsOf.Add(time.Minute))
	srv.seed(t, "at-end", "SIM1", demandEnd, 5, demandAsOf.Add(time.Hour)) // newest event, belongs to the NEXT window
	srv.seed(t, "other-site", "SIM2", demandStart.Add(time.Hour), 7, demandAsOf.Add(2*time.Hour))

	status, hdr, body := getPath(t, srv.Server, demandQuery("SIM1", "2026-10-05T08:00:00Z", "2026-10-05T16:00:00Z"))
	if status != http.StatusOK {
		t.Fatalf("status = %d: %s", status, body)
	}
	if ct := hdr.Get("Content-Type"); ct != "application/json" {
		t.Errorf("content-type = %q", ct)
	}
	got := decodeMap(t, body)
	want := map[string]any{
		"location": "SIM1", "window_start": "2026-10-05T08:00:00Z", "window_end": "2026-10-05T16:00:00Z",
		"orders": float64(2), "released_lines": float64(5), "source": "order-management",
		"as_of": "2026-10-04T10:15:30Z",
	}
	for k, v := range want {
		if got[k] != v {
			t.Errorf("%s = %v, want %v", k, got[k], v)
		}
	}
	if len(got) != len(want) {
		t.Errorf("unexpected fields in %v", got)
	}
}

func TestGetDemand_NoDataIsAnEmptyAnswerWithANullAsOf(t *testing.T) {
	srv := newDemandServer(t)
	status, _, body := getPath(t, srv.Server, demandQuery("SIM1", "2026-10-05T08:00:00Z", "2026-10-05T16:00:00Z"))
	if status != http.StatusOK {
		t.Fatalf("status = %d: %s", status, body)
	}
	got := decodeMap(t, body)
	asOf, present := got["as_of"]
	if !present || asOf != nil {
		t.Fatalf("as_of = %v (present %v), want an explicit null", asOf, present)
	}
	if got["orders"] != float64(0) || got["released_lines"] != float64(0) {
		t.Fatalf("body = %s", body)
	}
}

func TestGetDemand_Rejections(t *testing.T) {
	srv := newDemandServer(t)
	cases := []struct {
		name, path string
		status     int
		problem    string
	}{
		{"missing location", "/demand?window_start=2026-10-05T08:00:00Z&window_end=2026-10-05T16:00:00Z", 400, "missing-location"},
		{"blank location", demandQuery("", "2026-10-05T08:00:00Z", "2026-10-05T16:00:00Z"), 400, "missing-location"},
		{"missing window_start", "/demand?location=SIM1&window_end=2026-10-05T16:00:00Z", 400, "malformed-window-start"},
		{"malformed window_start", demandQuery("SIM1", "yesterday", "2026-10-05T16:00:00Z"), 400, "malformed-window-start"},
		{"malformed window_end", demandQuery("SIM1", "2026-10-05T08:00:00Z", "soon"), 400, "malformed-window-end"},
		{"empty window", demandQuery("SIM1", "2026-10-05T08:00:00Z", "2026-10-05T08:00:00Z"), 400, "invalid-capacity-window"},
		{"inverted window", demandQuery("SIM1", "2026-10-05T16:00:00Z", "2026-10-05T08:00:00Z"), 400, "invalid-capacity-window"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			status, hdr, body := getPath(t, srv.Server, tc.path)
			if status != tc.status {
				t.Fatalf("status = %d, want %d: %s", status, tc.status, body)
			}
			if ct := hdr.Get("Content-Type"); ct != "application/problem+json" {
				t.Errorf("content-type = %q, want RFC 7807", ct)
			}
			if got := problemType(t, body); got != tc.problem {
				t.Errorf("problem = %q, want %q", got, tc.problem)
			}
		})
	}
}

func TestGetDemand_RouteDoesNotExistWithoutTheDemandReadModel(t *testing.T) {
	srv, _ := newTestServer() // a Server built without GetExpectedDemand
	defer srv.Close()
	if status, _, _ := getPath(t, srv, demandQuery("SIM1", "2026-10-05T08:00:00Z", "2026-10-05T16:00:00Z")); status != http.StatusNotFound {
		t.Fatalf("status = %d, want 404 when the read model is not wired", status)
	}
}

// --- POST /capacity-plans: assigned_demand is optional ------------------------

func seedDemandPlanFixture(t *testing.T, srv *demandServer) {
	t.Helper()
	seedPlanFixture(t, srv.Server)
}

func seedOrdersInPlanWindow(t *testing.T, srv *demandServer, n int) {
	t.Helper()
	for i := 0; i < n; i++ {
		srv.seed(t, fmt.Sprintf("ord-%d", i), "PATH-ZONE-A", demandStart.Add(time.Duration(i%7)*time.Hour), 1, demandAsOf)
	}
}

func TestCreatePlan_OmittedDemandUsesOrdersAndSaysSo(t *testing.T) {
	srv := newDemandServer(t)
	seedDemandPlanFixture(t, srv)
	seedOrdersInPlanWindow(t, srv, 8500) // capacity over the 8h window is 8000
	srv.seed(t, "at-end", "PATH-ZONE-A", demandEnd, 1, demandAsOf)

	status, _, body := postJSON(t, srv.Server, "/capacity-plans", planBody(nil))
	if status != http.StatusCreated {
		t.Fatalf("status = %d: %s", status, body)
	}
	got := decodeMap(t, body)
	if got["assigned_demand"] != float64(8500) || got["demand_source"] != "orders" || got["shortage"] != float64(500) {
		t.Fatalf("plan = demand %v source %v shortage %v", got["assigned_demand"], got["demand_source"], got["shortage"])
	}

	// The provenance is part of the stored plan, not only of the create response.
	id, _ := got["id"].(string)
	status, _, body = getPath(t, srv.Server, "/capacity-plans/"+id)
	if status != http.StatusOK || decodeMap(t, body)["demand_source"] != "orders" {
		t.Fatalf("GET plan = %d %s", status, body)
	}
}

func TestCreatePlan_ExplicitDemandWinsAndIsLabelledRequest(t *testing.T) {
	srv := newDemandServer(t)
	seedDemandPlanFixture(t, srv)
	seedOrdersInPlanWindow(t, srv, 300)

	for _, demandValue := range []any{12000, 0} {
		status, _, body := postJSON(t, srv.Server, "/capacity-plans", planBody(demandValue))
		if status != http.StatusCreated {
			t.Fatalf("status = %d: %s", status, body)
		}
		got := decodeMap(t, body)
		if got["assigned_demand"] != float64(demandValue.(int)) || got["demand_source"] != "request" {
			t.Fatalf("with explicit %v: demand %v source %v", demandValue, got["assigned_demand"], got["demand_source"])
		}
	}
}

func TestCreatePlan_OmittedDemandWithoutOrderDataKeepsTodays422(t *testing.T) {
	srv := newDemandServer(t)
	seedDemandPlanFixture(t, srv)
	srv.seed(t, "elsewhere", "OTHER-SITE", demandStart.Add(time.Hour), 1, demandAsOf)
	srv.seed(t, "at-end", "PATH-ZONE-A", demandEnd, 1, demandAsOf)

	for name, body := range map[string]map[string]any{
		"absent": planBody(nil),
		"null": func() map[string]any {
			b := planBody(nil)
			b["assigned_demand"] = nil
			return b
		}(),
	} {
		status, hdr, resp := postJSON(t, srv.Server, "/capacity-plans", body)
		if status != http.StatusUnprocessableEntity {
			t.Fatalf("%s: status = %d: %s", name, status, resp)
		}
		if hdr.Get("Content-Type") != "application/problem+json" || problemType(t, resp) != "missing-assigned-demand" {
			t.Fatalf("%s: %s / %s", name, hdr.Get("Content-Type"), resp)
		}
		var p struct{ Title, Detail string }
		if err := json.Unmarshal(resp, &p); err != nil || p.Title != "assigned_demand is required" || p.Detail != "assigned_demand (orders) must be provided" {
			t.Fatalf("%s: problem body = %s", name, resp)
		}
	}
}

func TestCreatePlan_NegativeExplicitDemandIsStillRejected(t *testing.T) {
	srv := newDemandServer(t)
	seedDemandPlanFixture(t, srv)
	seedOrdersInPlanWindow(t, srv, 10)
	status, _, body := postJSON(t, srv.Server, "/capacity-plans", planBody(-5))
	if status != http.StatusUnprocessableEntity || problemType(t, body) != "negative-assigned-demand" {
		t.Fatalf("status = %d: %s", status, body)
	}
}
