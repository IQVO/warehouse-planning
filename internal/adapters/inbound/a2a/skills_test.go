package a2a

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/a2aproject/a2a-go/v2/a2a"
	"github.com/a2aproject/a2a-go/v2/a2aclient"
	"github.com/a2aproject/a2a-go/v2/a2asrv"

	outboundkafka "github.com/claudioed/warehouse-planning/internal/adapters/outbound/kafka"
	"github.com/claudioed/warehouse-planning/internal/adapters/outbound/memory"
	"github.com/claudioed/warehouse-planning/internal/application/usecases"
	"github.com/claudioed/warehouse-planning/internal/domain/demand"
	"github.com/claudioed/warehouse-planning/internal/domain/processcapacity"
	"github.com/claudioed/warehouse-planning/internal/domain/processpath"
)

const (
	winStart = "2026-10-05T08:00:00Z"
	winEnd   = "2026-10-05T16:00:00Z"
)

// snakeCaseKeys are the REST/MCP argument names this surface does NOT accept;
// no answer or card text may teach them.
var snakeCaseKeys = []string{
	"assigned_demand", "units_per_order", "packages_per_order",
	"window_start", "window_end", "path_id", "site_id", "warehouse_id",
}

// harness builds Deps over in-memory repos wired exactly as cmd/a2a wires them
// (minus Postgres), seeding through the real use cases.
type harness struct {
	deps   Deps
	outbox *memory.OutboxRepo
	plans  *memory.CapacityPlanRepo
	demand *memory.OrderDemandRepo
	reg    *usecases.RegisterProcessCapacityConstraint
	paths  *usecases.RegisterProcessPath
}

func newHarness() *harness {
	pc, paths := memory.NewProcessCapacityRepo(), memory.NewProcessPathRepo()
	plans, outboxRepo, orders := memory.NewCapacityPlanRepo(), memory.NewOutboxRepo(), memory.NewOrderDemandRepo()
	uow := memory.NewUnitOfWork(pc, plans, outboxRepo)
	pathCapacity := &usecases.GetProcessPathCapacity{
		ProcessPaths: paths, ProcessCapacities: pc,
		StationStandards: memory.NewStationStandardRepo(), Tally: memory.NewStorageTallyRepo(),
	}
	return &harness{
		deps: Deps{
			GetProcessPathCapacity: pathCapacity,
			CreateCapacityPlan: &usecases.CreateCapacityPlan{
				PathCapacity: pathCapacity, Plans: plans, Outbox: outboxRepo, Encoder: outboundkafka.NewFanoutEncoder(),
				UnitOfWork: uow, Demand: &usecases.GetExpectedDemand{Demand: orders},
			},
		},
		outbox: outboxRepo, plans: plans, demand: orders,
		reg:   &usecases.RegisterProcessCapacityConstraint{Repo: pc},
		paths: &usecases.RegisterProcessPath{Repo: paths},
	}
}

// seedSection43 registers the design doc's section-43 capacities and path on
// location: PICK 4000 UNIT/h, REBIN 2500 UNIT/h, PACK 1800 PACKAGE/h on
// PICK -> REBIN -> PACK, which at 2.5 units and 1 package per order is
// 1000 ORDER/h bound by REBIN.
func (h *harness) seedSection43(t *testing.T, location, pathID string) {
	t.Helper()
	start, _ := time.Parse(time.RFC3339, winStart)
	end, _ := time.Parse(time.RFC3339, winEnd)
	for _, c := range []struct {
		process processcapacity.ProcessType
		qty     float64
		unit    processcapacity.CapacityUnit
	}{{"PICK", 4000, "UNIT"}, {"REBIN", 2500, "UNIT"}, {"PACK", 1800, "PACKAGE"}} {
		if _, err := h.reg.Handle(context.Background(), usecases.RegisterProcessCapacityConstraintCommand{
			ProcessType: c.process, Location: location, WindowStart: start, WindowEnd: end,
			ConstraintType: processcapacity.ConstraintType("LABOR"), Quantity: c.qty, Unit: c.unit, Period: time.Hour,
		}); err != nil {
			t.Fatalf("seed %s: %v", c.process, err)
		}
	}
	steps := []processpath.ProcessType{"PICK", "REBIN", "PACK"}
	if _, err := h.paths.Handle(context.Background(), usecases.RegisterProcessPathCommand{ID: pathID, Name: "Tote path", Steps: steps}); err != nil {
		t.Fatalf("seed path: %v", err)
	}
}

func (h *harness) seedOrders(t *testing.T, location string, n int) {
	t.Helper()
	promise, _ := time.Parse(time.RFC3339, "2026-10-05T10:00:00Z")
	for i := range n {
		o, err := demand.NewOrder(demand.OrderParams{OrderID: "o" + string(rune('a'+i)), Location: location, PromiseAt: promise, ReleasedLines: 2, AsOf: promise})
		if err != nil {
			t.Fatal(err)
		}
		if _, err := h.demand.Upsert(context.Background(), o); err != nil {
			t.Fatal(err)
		}
	}
}

// ---- the wire: JSON-RPC over HTTP, driven by the SDK's own client ----------

// wireEnvelope is a TEST-LOCAL copy of the answer contract. If the adapter's
// envelope drifts, this suite fails instead of silently following it.
type wireEnvelope struct {
	Skill string `json:"skill"`
	Data  struct {
		PathCapacity *struct {
			PathID               string  `json:"pathId"`
			Location             string  `json:"location"`
			WindowStart          string  `json:"windowStart"`
			WindowEnd            string  `json:"windowEnd"`
			NormalizedRate       float64 `json:"normalizedRate"`
			NormalizedUnit       string  `json:"normalizedUnit"`
			BottleneckStep       string  `json:"bottleneckStep"`
			BottleneckConstraint string  `json:"bottleneckConstraint"`
			Steps                []struct {
				Step              string  `json:"step"`
				NormalizedRate    float64 `json:"normalizedRate"`
				BindingConstraint string  `json:"bindingConstraint"`
			} `json:"steps"`
			Warnings []string `json:"warnings"`
		} `json:"pathCapacity"`
		CapacityPlan *struct {
			PlanID               string   `json:"planId"`
			WarehouseID          string   `json:"warehouseId"`
			SiteID               string   `json:"siteId"`
			Location             string   `json:"location"`
			PathID               string   `json:"pathId"`
			Status               string   `json:"status"`
			AssignedDemand       float64  `json:"assignedDemand"`
			DemandSource         string   `json:"demandSource"`
			PathCapacity         float64  `json:"pathCapacity"`
			CapacityOverWindow   float64  `json:"capacityOverWindow"`
			Shortage             float64  `json:"shortage"`
			BottleneckStep       string   `json:"bottleneckStep"`
			BottleneckConstraint string   `json:"bottleneckConstraint"`
			Warnings             []string `json:"warnings"`
		} `json:"capacityPlan"`
	} `json:"data"`
	Problem *struct {
		Type   string `json:"type"`
		Title  string `json:"title"`
		Detail string `json:"detail"`
		Status int    `json:"status"`
	} `json:"problem"`
}

type wire struct {
	client *a2aclient.Client
	// seen collects every text an agent could read (problem title/detail and
	// the reply's text part), for the vocabulary-leak check.
	seen []string
}

func newWire(t *testing.T, deps Deps) *wire {
	t.Helper()
	srv := httptest.NewServer(Handler(NewExecutor(deps)))
	t.Cleanup(srv.Close)
	card := Card(Config{AgentURL: srv.URL + "/", Version: "test"})
	client, err := a2aclient.NewFromCard(context.Background(), card)
	if err != nil {
		t.Fatalf("NewFromCard: %v", err)
	}
	return &wire{client: client}
}

// call sends one message part and returns the decoded envelope and the
// one-line text summary of the agent's reply.
func (w *wire) call(t *testing.T, part *a2a.Part) (wireEnvelope, string) {
	t.Helper()
	resp, err := w.client.SendMessage(context.Background(), &a2a.SendMessageRequest{Message: a2a.NewMessage(a2a.MessageRoleUser, part)})
	if err != nil {
		t.Fatalf("SendMessage: %v", err)
	}
	msg, ok := resp.(*a2a.Message)
	if !ok || len(msg.Parts) < 2 {
		t.Fatalf("reply = %T, want a message with a data and a text part", resp)
	}
	data, ok := msg.Parts[0].Content.(a2a.Data)
	if !ok {
		t.Fatalf("first reply part is %T, want data", msg.Parts[0].Content)
	}
	raw, _ := json.Marshal(data.Value)
	var env wireEnvelope
	if err := json.Unmarshal(raw, &env); err != nil {
		t.Fatalf("reply is not the skill envelope: %v (%s)", err, raw)
	}
	text, _ := msg.Parts[1].Content.(a2a.Text)
	w.seen = append(w.seen, string(text))
	if env.Problem != nil {
		w.seen = append(w.seen, env.Problem.Title, env.Problem.Detail)
	}
	return env, string(text)
}

func (w *wire) skill(t *testing.T, skill string, args map[string]any) (wireEnvelope, string) {
	t.Helper()
	return w.call(t, a2a.NewDataPart(map[string]any{"skill": skill, "arguments": args}))
}

func wantProblem(t *testing.T, env wireEnvelope, status int, slug string) {
	t.Helper()
	if env.Problem == nil {
		t.Fatalf("expected a %d problem, got data: %+v", status, env.Data)
	}
	if env.Problem.Status != status || env.Problem.Type != problemBaseURI+slug {
		t.Fatalf("problem = %+v, want %d %s", env.Problem, status, slug)
	}
}

func capacityArgs(location, pathID string) map[string]any {
	return map[string]any{
		"pathId": pathID, "location": location, "windowStart": winStart, "windowEnd": winEnd,
		"unitsPerOrder": 2.5, "packagesPerOrder": 1,
	}
}

func planArgs(location, pathID string, extra map[string]any) map[string]any {
	args := map[string]any{
		"warehouseId": "WH-1", "siteId": "SIM1", "location": location, "pathId": pathID,
		"windowStart": winStart, "windowEnd": winEnd, "unitsPerOrder": 2.5, "packagesPerOrder": 1,
	}
	for k, v := range extra {
		args[k] = v
	}
	return args
}

func TestWirePathCapacityReadsTheSection43Numbers(t *testing.T) {
	h := newHarness()
	h.seedSection43(t, "FC01", "tote-path")
	w := newWire(t, h.deps)

	env, text := w.skill(t, skillPathCapacity, capacityArgs("FC01", "tote-path"))
	pc := env.Data.PathCapacity
	if env.Problem != nil || pc == nil || env.Skill != skillPathCapacity {
		t.Fatalf("envelope = %+v", env)
	}
	if pc.NormalizedRate != 1000 || pc.NormalizedUnit != "ORDER" || pc.BottleneckStep != "REBIN" || pc.BottleneckConstraint != "LABOR" {
		t.Fatalf("path capacity = %+v, want 1000 ORDER/h bound by REBIN (LABOR)", pc)
	}
	if len(pc.Steps) != 3 || pc.Warnings == nil || pc.WindowStart != winStart || pc.WindowEnd != winEnd {
		t.Fatalf("steps/warnings/window = %+v", pc)
	}
	if text != "tote-path at FC01: 1000 ORDER per hour, bottleneck REBIN (LABOR)" {
		t.Fatalf("summary = %q", text)
	}
	if got, _ := h.plans.FindByID(context.Background(), "x"); got != nil || len(h.outbox.Messages()) != 0 {
		t.Fatal("a read skill must persist nothing")
	}
}

func TestWireEvaluateCapacityPlanRecordsADraftAndOutboxesItsEvents(t *testing.T) {
	h := newHarness()
	h.seedSection43(t, "FC01", "tote-path")
	w := newWire(t, h.deps)

	env, text := w.skill(t, skillEvaluateCapacityPlan, planArgs("FC01", "tote-path", map[string]any{"assignedDemand": 12000}))
	p := env.Data.CapacityPlan
	if env.Problem != nil || p == nil || env.Skill != skillEvaluateCapacityPlan {
		t.Fatalf("envelope = %+v", env)
	}
	assertSection43Draft(t, p.Status, p.Shortage, p.CapacityOverWindow, p.PathCapacity, p.BottleneckStep)
	if p.AssignedDemand != 12000 || p.DemandSource != "request" || p.SiteID != "SIM1" {
		t.Fatalf("plan = %+v, want demand 12000 from the request at SIM1", p)
	}
	if !strings.HasPrefix(text, "plan "+p.PlanID+" DRAFT: 12000 orders against 8000 capacity") {
		t.Fatalf("summary = %q", text)
	}

	stored, err := h.plans.FindByID(context.Background(), p.PlanID)
	if err != nil || stored == nil || string(stored.Status()) != "DRAFT" {
		t.Fatalf("stored plan = %v, %v: the delegated write really happened", stored, err)
	}
	assertOnlyCreatedEvents(t, h.outbox)
}

// assertSection43Draft pins the design doc's section-43 numbers: 12000 orders
// over 8h on PICK -> REBIN -> PACK is a DRAFT short by 4000, bound by REBIN.
func assertSection43Draft(t *testing.T, status string, shortage, overWindow, pathCapacity float64, bottleneck string) {
	t.Helper()
	if status != "DRAFT" || shortage != 4000 || overWindow != 8000 || pathCapacity != 1000 || bottleneck != "REBIN" {
		t.Fatalf("plan = %s shortage %g over-window %g path %g bottleneck %s, want the section-43 DRAFT: shortage 4000, bound by REBIN",
			status, shortage, overWindow, pathCapacity, bottleneck)
	}
}

// assertOnlyCreatedEvents: a DRAFT plan outboxes its CapacityPlanCreated events
// and nothing that publishes it.
func assertOnlyCreatedEvents(t *testing.T, outbox *memory.OutboxRepo) {
	t.Helper()
	var created int
	for _, m := range outbox.Messages() {
		if strings.HasSuffix(m.EventType, ".capacityplan.CapacityPlanCreated") {
			created++
		}
		if strings.Contains(m.EventType, "Published") {
			t.Fatalf("event %s: this skill must not publish the plan", m.EventType)
		}
	}
	if created == 0 {
		t.Fatalf("no CapacityPlanCreated event reached the outbox: %d messages", len(outbox.Messages()))
	}
}

func TestWireOmittedDemandIsDefaultedFromExpectedOrders(t *testing.T) {
	h := newHarness()
	h.seedSection43(t, "FC01", "tote-path")
	h.seedOrders(t, "FC01", 3)
	w := newWire(t, h.deps)

	env, _ := w.skill(t, skillEvaluateCapacityPlan, planArgs("FC01", "tote-path", nil))
	p := env.Data.CapacityPlan
	if env.Problem != nil || p == nil || p.AssignedDemand != 3 || p.DemandSource != "orders" || p.Shortage != 0 {
		t.Fatalf("plan = %+v (problem %+v), want demand 3 sourced from orders", p, env.Problem)
	}

	// An explicit 0 is a real figure, never "omitted".
	env, _ = w.skill(t, skillEvaluateCapacityPlan, planArgs("FC01", "tote-path", map[string]any{"assignedDemand": 0}))
	if p := env.Data.CapacityPlan; env.Problem != nil || p == nil || p.AssignedDemand != 0 || p.DemandSource != "request" {
		t.Fatalf("explicit zero = %+v (problem %+v)", p, env.Problem)
	}
}

func TestWireDomainRejectionsAreAnsweredNotRaisedWithRESTClassification(t *testing.T) {
	h := newHarness()
	h.seedSection43(t, "FC01", "tote-path")
	w := newWire(t, h.deps)
	before := len(h.outbox.Messages())

	cases := []struct {
		name   string
		skill  string
		args   map[string]any
		status int
		slug   string
	}{
		{"read: unknown path", skillPathCapacity, capacityArgs("FC01", "ghost"), 404, "process-path-not-found"},
		{"write: unknown path", skillEvaluateCapacityPlan, planArgs("FC01", "ghost", map[string]any{"assignedDemand": 1}), 404, "process-path-not-found"},
		{"read: location without capacity", skillPathCapacity, capacityArgs("NOWHERE", "tote-path"), 422, "missing-step-capacity"},
		{"write: location without capacity", skillEvaluateCapacityPlan, planArgs("NOWHERE", "tote-path", map[string]any{"assignedDemand": 1}), 422, "missing-step-capacity"},
		{"read: inverted window", skillPathCapacity, merge(capacityArgs("FC01", "tote-path"), map[string]any{"windowStart": winEnd, "windowEnd": winStart}), 422, "invalid-capacity-window"},
		{"write: inverted window", skillEvaluateCapacityPlan, planArgs("FC01", "tote-path", map[string]any{"assignedDemand": 1, "windowStart": winEnd, "windowEnd": winStart}), 422, "invalid-capacity-window"},
		{"read: missing conversion factor", skillPathCapacity, map[string]any{"pathId": "tote-path", "location": "FC01", "windowStart": winStart, "windowEnd": winEnd}, 422, "missing-conversion-factor"},
		{"read: non-positive conversion factor", skillPathCapacity, merge(capacityArgs("FC01", "tote-path"), map[string]any{"unitsPerOrder": 0}), 422, "non-positive-conversion-factor"},
		{"write: negative demand", skillEvaluateCapacityPlan, planArgs("FC01", "tote-path", map[string]any{"assignedDemand": -5}), 422, "negative-assigned-demand"},
		{"write: no demand and no orders", skillEvaluateCapacityPlan, planArgs("FC01", "tote-path", nil), 422, "missing-assigned-demand"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			env, _ := w.skill(t, tc.skill, tc.args)
			wantProblem(t, env, tc.status, tc.slug)
		})
	}
	if got := len(h.outbox.Messages()); got != before {
		t.Fatalf("outbox grew from %d to %d: a rejected skill must write nothing", before, got)
	}
}

func merge(base, extra map[string]any) map[string]any {
	out := make(map[string]any, len(base)+len(extra))
	for k, v := range base {
		out[k] = v
	}
	for k, v := range extra {
		out[k] = v
	}
	return out
}

func TestWireArgumentValidation(t *testing.T) {
	h := newHarness()
	h.seedSection43(t, "FC01", "tote-path")
	w := newWire(t, h.deps)

	without := func(args map[string]any, key string) map[string]any {
		out := merge(args, nil)
		delete(out, key)
		return out
	}
	read, write := capacityArgs("FC01", "tote-path"), planArgs("FC01", "tote-path", map[string]any{"assignedDemand": 1})
	cases := map[string]struct {
		skill string
		args  map[string]any
		slug  string
	}{
		"read: missing pathId":         {skillPathCapacity, without(read, "pathId"), "invalid-argument"},
		"read: missing location":       {skillPathCapacity, without(read, "location"), "invalid-argument"},
		"read: missing windowStart":    {skillPathCapacity, without(read, "windowStart"), "invalid-argument"},
		"read: missing windowEnd":      {skillPathCapacity, without(read, "windowEnd"), "invalid-argument"},
		"read: blank pathId":           {skillPathCapacity, merge(read, map[string]any{"pathId": "  "}), "invalid-argument"},
		"read: malformed windowStart":  {skillPathCapacity, merge(read, map[string]any{"windowStart": "yesterday"}), "malformed-window-start"},
		"read: malformed windowEnd":    {skillPathCapacity, merge(read, map[string]any{"windowEnd": "2026-10-05"}), "malformed-window-end"},
		"read: snake_case name":        {skillPathCapacity, merge(without(read, "pathId"), map[string]any{"path_id": "tote-path"}), "invalid-argument"},
		"read: typo'd optional":        {skillPathCapacity, merge(read, map[string]any{"unitsPerOrdr": 2}), "invalid-argument"},
		"read: wrong type":             {skillPathCapacity, merge(read, map[string]any{"unitsPerOrder": "two"}), "invalid-argument"},
		"write: missing warehouseId":   {skillEvaluateCapacityPlan, without(write, "warehouseId"), "invalid-argument"},
		"write: missing siteId":        {skillEvaluateCapacityPlan, without(write, "siteId"), "invalid-argument"},
		"write: missing location":      {skillEvaluateCapacityPlan, without(write, "location"), "invalid-argument"},
		"write: missing pathId":        {skillEvaluateCapacityPlan, without(write, "pathId"), "invalid-argument"},
		"write: missing windowStart":   {skillEvaluateCapacityPlan, without(write, "windowStart"), "invalid-argument"},
		"write: malformed windowEnd":   {skillEvaluateCapacityPlan, merge(write, map[string]any{"windowEnd": "soon"}), "malformed-window-end"},
		"write: unknown argument":      {skillEvaluateCapacityPlan, merge(write, map[string]any{"publish": true}), "invalid-argument"},
		"write: snake_case demand":     {skillEvaluateCapacityPlan, merge(without(write, "assignedDemand"), map[string]any{"assigned_demand": 5}), "invalid-argument"},
		"write: typo'd assignedDemand": {skillEvaluateCapacityPlan, merge(without(write, "assignedDemand"), map[string]any{"assignedDemnd": 5}), "invalid-argument"},
	}
	before := len(h.outbox.Messages())
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			env, _ := w.skill(t, tc.skill, tc.args)
			wantProblem(t, env, 400, tc.slug)
		})
	}
	if got := len(h.outbox.Messages()); got != before {
		t.Fatalf("outbox grew from %d to %d: invalid arguments must write nothing", before, got)
	}
}

func TestWireRequestShapeProblems(t *testing.T) {
	h := newHarness()
	h.seedSection43(t, "FC01", "tote-path")
	w := newWire(t, h.deps)

	env, _ := w.call(t, a2a.NewTextPart("not json at all"))
	wantProblem(t, env, 400, "malformed-skill-request")

	env, _ = w.call(t, a2a.NewDataPart(map[string]any{"arguments": map[string]any{}}))
	wantProblem(t, env, 400, "malformed-skill-request")

	env, _ = w.call(t, a2a.NewDataPart(map[string]any{"skill": skillPathCapacity, "arguments": map[string]any{}, "extra": 1}))
	wantProblem(t, env, 400, "malformed-skill-request")

	// Trailing data after the JSON document is refused, not ignored.
	env, _ = w.call(t, a2a.NewTextPart(`{"skill":"path_capacity","arguments":{}} {"skill":"evaluate_capacity_plan"}`))
	wantProblem(t, env, 400, "malformed-skill-request")

	env, _ = w.skill(t, "launch_rocket", map[string]any{})
	wantProblem(t, env, 400, "unknown-skill")

	// A skill the MCP surface offers but this agent deliberately does not.
	env, _ = w.skill(t, "publish_capacity_plan", map[string]any{"id": "x"})
	wantProblem(t, env, 400, "unknown-skill")

	// A text part holding the same JSON is accepted (documented contract).
	env, _ = w.call(t, a2a.NewTextPart(`{"skill":"path_capacity","arguments":{"pathId":"tote-path","location":"FC01","windowStart":"`+winStart+`","windowEnd":"`+winEnd+`","unitsPerOrder":2.5,"packagesPerOrder":1}}`))
	if env.Problem != nil || env.Data.PathCapacity == nil {
		t.Fatalf("a text part holding the skill JSON must be accepted, got %+v", env)
	}
}

// TestWireNeverTeachesAKeyThisSurfaceDoesNotOffer: the shared domain error for
// an omitted demand names assigned_demand, a REST/MCP key. No answer an agent
// can receive may mention any snake_case name, or the agent that follows it
// would be refused as an unknown argument.
func TestWireNeverTeachesAKeyThisSurfaceDoesNotOffer(t *testing.T) {
	h := newHarness()
	h.seedSection43(t, "FC01", "tote-path")
	w := newWire(t, h.deps)

	env, text := w.skill(t, skillEvaluateCapacityPlan, planArgs("FC01", "tote-path", nil))
	wantProblem(t, env, 422, "missing-assigned-demand")
	if !strings.Contains(env.Problem.Detail, "assignedDemand") {
		t.Fatalf("detail = %q, want the A2A key assignedDemand", env.Problem.Detail)
	}
	env, _ = w.skill(t, skillEvaluateCapacityPlan, planArgs("FC01", "tote-path", map[string]any{"assignedDemand": -1}))
	wantProblem(t, env, 422, "negative-assigned-demand")
	_, _ = w.skill(t, skillPathCapacity, capacityArgs("FC01", "ghost"))
	_, _ = w.skill(t, skillPathCapacity, merge(capacityArgs("FC01", "tote-path"), map[string]any{"unitsPerOrder": 0}))
	_, _ = w.skill(t, skillPathCapacity, map[string]any{"pathId": "tote-path", "location": "FC01", "windowStart": winStart, "windowEnd": winEnd})
	_, _ = w.skill(t, skillPathCapacity, merge(capacityArgs("NOWHERE", "tote-path"), nil))

	for _, s := range append(w.seen, text) {
		for _, key := range snakeCaseKeys {
			if strings.Contains(s, key) {
				t.Fatalf("an answer teaches %q, a name this surface refuses: %q", key, s)
			}
		}
	}
}

// TestCardIsServedAndDialable proves discovery end to end: the card handler
// serves the card at the well-known path and a client built from the FETCHED
// card reaches the agent.
func TestCardIsServedAndDialable(t *testing.T) {
	h := newHarness()
	h.seedSection43(t, "FC01", "tote-path")
	mux := http.NewServeMux()
	mux.Handle("/", Handler(NewExecutor(h.deps)))
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)
	mux.Handle(a2asrv.WellKnownAgentCardPath, a2asrv.NewStaticAgentCardHandler(Card(Config{AgentURL: srv.URL + "/", Version: "test"})))

	res, err := http.Get(srv.URL + a2asrv.WellKnownAgentCardPath)
	if err != nil || res.StatusCode != http.StatusOK {
		t.Fatalf("card fetch = %v / %v", res, err)
	}
	defer func() { _ = res.Body.Close() }()
	var card a2a.AgentCard
	if err := json.NewDecoder(res.Body).Decode(&card); err != nil {
		t.Fatal(err)
	}
	client, err := a2aclient.NewFromCard(context.Background(), &card)
	if err != nil {
		t.Fatal(err)
	}
	w := &wire{client: client}
	env, _ := w.skill(t, skillPathCapacity, capacityArgs("FC01", "tote-path"))
	if env.Data.PathCapacity == nil {
		t.Fatalf("a client built from the served card could not call the agent: %+v", env)
	}
}
