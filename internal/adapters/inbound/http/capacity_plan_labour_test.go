package http_test

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"reflect"
	"testing"
	"time"

	inboundhttp "github.com/claudioed/warehouse-planning/internal/adapters/inbound/http"
	outboundkafka "github.com/claudioed/warehouse-planning/internal/adapters/outbound/kafka"
	"github.com/claudioed/warehouse-planning/internal/adapters/outbound/memory"
	"github.com/claudioed/warehouse-planning/internal/application/usecases"
	"github.com/claudioed/warehouse-planning/internal/domain/laborstandard"
)

// newLabourPlanServer is newPlanServer with the labour standards copy wired
// (docs/adr/0022): PACK 90 s for every site, PICK 12 s scoped to SIM1.
func newLabourPlanServer(t *testing.T) *planServer {
	t.Helper()
	pcs, flows, plans, ob := memory.NewProcessCapacityRepo(), memory.NewProcessFlowRepo(), memory.NewCapacityPlanRepo(), memory.NewOutboxRepo()
	uow := memory.NewUnitOfWork(pcs, plans, ob)
	enc := outboundkafka.NewEncoder()
	standards := memory.NewLaborStandardRepo()
	from := time.Date(2026, 10, 1, 0, 0, 0, 0, time.UTC)
	for _, p := range []laborstandard.Params{
		{StandardID: "std-pack", TaskType: "PACK", ExpectedSeconds: 90, EffectiveFrom: from, AsOf: from},
		{StandardID: "std-pick", TaskType: "PICK", SiteCode: "SIM1", ExpectedSeconds: 12, EffectiveFrom: from, AsOf: from},
	} {
		s, err := laborstandard.New(p)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := standards.Record(context.Background(), s); err != nil {
			t.Fatal(err)
		}
	}
	pathCapacity := &usecases.GetProcessPathCapacity{ProcessPaths: &usecases.FlowPathFinder{Flows: flows}, ProcessCapacities: pcs, StationStandards: memory.NewStationStandardRepo(), Tally: memory.NewStorageTallyRepo()}
	s := &inboundhttp.Server{
		RegisterProcessCapacityConstraint: &usecases.RegisterProcessCapacityConstraint{Repo: pcs},
		ProcessCapacities:                 pcs,
		GetProcessPathCapacity:            pathCapacity,
		CreateCapacityPlan: &usecases.CreateCapacityPlan{
			PathCapacity: pathCapacity, Plans: plans, Outbox: ob, Encoder: enc, UnitOfWork: uow,
			LaborStandards: standards,
		},
		PublishCapacityPlan: &usecases.PublishCapacityPlan{Plans: plans, Outbox: ob, Encoder: enc, UnitOfWork: uow},
		CapacityPlans:       plans,
	}
	srv := httptest.NewServer(inboundhttp.NewRouter(s))
	t.Cleanup(srv.Close)
	return &planServer{Server: srv, outbox: ob, flows: flows}
}

type labourItemJSON struct {
	ProcessType          string  `json:"process_type"`
	StandardSeconds      float64 `json:"standard_seconds"`
	StandardSiteScoped   bool    `json:"standard_site_scoped"`
	Tasks                float64 `json:"tasks"`
	RequiredLabourHours  float64 `json:"required_labour_hours"`
	RequiredAverageHeads float64 `json:"required_average_heads"`
	Known                bool    `json:"known"`
	Reason               *string `json:"reason"`
}

func decodeLabour(t *testing.T, body []byte) []labourItemJSON {
	t.Helper()
	var p struct {
		LabourRequirement *[]labourItemJSON `json:"labour_requirement"`
	}
	if err := json.Unmarshal(body, &p); err != nil {
		t.Fatalf("decode: %v (%s)", err, body)
	}
	if p.LabourRequirement == nil {
		t.Fatalf("labour_requirement is absent or null: %s", body)
	}
	return *p.LabourRequirement
}

func strptr(s string) *string { return &s }

func TestHandler_CapacityPlan_LabourRequirementPerStep(t *testing.T) {
	srv := newLabourPlanServer(t)
	seedPlanFixture(t, srv.Server, srv.flows)
	req := planBody(12000)
	req["tasks_per_order"] = map[string]any{"PICK": 3, "PACK": 1}
	status, _, body := postJSON(t, srv.Server, "/capacity-plans", req)
	if status != http.StatusCreated {
		t.Fatalf("create = %d: %s", status, body)
	}
	// The worked example is untouched by the requirement.
	plan := decodePlan(t, body)
	assertWorkedExamplePlan(t, plan, "DRAFT")
	want := []labourItemJSON{
		{ProcessType: "PICK", StandardSeconds: 12, StandardSiteScoped: true, Tasks: 36000, RequiredLabourHours: 120, RequiredAverageHeads: 15, Known: true},
		{ProcessType: "REBIN", Reason: strptr("no-labor-standard")},
		{ProcessType: "PACK", StandardSeconds: 90, Tasks: 12000, RequiredLabourHours: 300, RequiredAverageHeads: 37.5, Known: true},
	}
	if got := decodeLabour(t, body); !reflect.DeepEqual(got, want) {
		t.Fatalf("POST labour_requirement = %+v\nwant %+v", got, want)
	}
	status, _, body = getPath(t, srv.Server, "/capacity-plans/"+plan.ID)
	if status != http.StatusOK {
		t.Fatalf("get = %d: %s", status, body)
	}
	if got := decodeLabour(t, body); !reflect.DeepEqual(got, want) {
		t.Fatalf("GET labour_requirement = %+v\nwant %+v", got, want)
	}
}

func TestHandler_CapacityPlan_WithoutFactorsEveryStepSaysWhyItIsUnknown(t *testing.T) {
	srv := newLabourPlanServer(t)
	seedPlanFixture(t, srv.Server, srv.flows)
	status, _, body := postJSON(t, srv.Server, "/capacity-plans", planBody(12000))
	if status != http.StatusCreated {
		t.Fatalf("create = %d: %s", status, body)
	}
	want := []labourItemJSON{
		{ProcessType: "PICK", StandardSeconds: 12, StandardSiteScoped: true, Reason: strptr("no-task-factor")},
		{ProcessType: "REBIN", Reason: strptr("no-labor-standard")},
		{ProcessType: "PACK", StandardSeconds: 90, Reason: strptr("no-task-factor")},
	}
	if got := decodeLabour(t, body); !reflect.DeepEqual(got, want) {
		t.Fatalf("labour_requirement = %+v\nwant %+v", got, want)
	}
}

func TestHandler_CapacityPlan_LegacyPlanWithoutStandardsIsUnchanged(t *testing.T) {
	srv := newPlanServer(t)
	seedPlanFixture(t, srv.Server, srv.flows)
	status, _, body := postJSON(t, srv.Server, "/capacity-plans", planBody(12000))
	if status != http.StatusCreated {
		t.Fatalf("create = %d: %s", status, body)
	}
	assertWorkedExamplePlan(t, decodePlan(t, body), "DRAFT")
	for _, item := range decodeLabour(t, body) {
		if item.Known || item.Reason == nil || *item.Reason != "no-labor-standard" || item.Tasks != 0 || item.RequiredLabourHours != 0 {
			t.Fatalf("a step without a standard = %+v, want known false / no-labor-standard / zero figures", item)
		}
	}
	assertOutboxTypes(t, srv.outbox, "CapacityPlanCreated")
}

func TestHandler_CapacityPlan_NonPositiveTaskFactorIs422(t *testing.T) {
	srv := newLabourPlanServer(t)
	seedPlanFixture(t, srv.Server, srv.flows)
	for _, factor := range []any{0, -1.5} {
		req := planBody(12000)
		req["tasks_per_order"] = map[string]any{"PACK": factor}
		status, headers, body := postJSON(t, srv.Server, "/capacity-plans", req)
		if status != http.StatusUnprocessableEntity || headers.Get("Content-Type") != "application/problem+json" || problemType(t, body) != "non-positive-tasks-per-order" {
			t.Errorf("tasks_per_order PACK %v = %d %s", factor, status, body)
		}
	}
	assertOutboxTypes(t, srv.outbox)
}
