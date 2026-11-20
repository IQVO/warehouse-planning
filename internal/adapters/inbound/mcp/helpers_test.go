package mcp_test

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	sdk "github.com/modelcontextprotocol/go-sdk/mcp"

	inboundmcp "github.com/claudioed/warehouse-planning/internal/adapters/inbound/mcp"
	outboundkafka "github.com/claudioed/warehouse-planning/internal/adapters/outbound/kafka"
	"github.com/claudioed/warehouse-planning/internal/adapters/outbound/memory"
	"github.com/claudioed/warehouse-planning/internal/application/usecases"
	"github.com/claudioed/warehouse-planning/internal/domain/processflow"
)

const (
	winStart = "2026-10-05T08:00:00Z"
	winEnd   = "2026-10-05T16:00:00Z"
)

// harness is a connected in-memory MCP client over the real server wired to
// in-memory repos, exactly as cmd/mcp wires them (minus Postgres).
type harness struct {
	session *sdk.ClientSession
	outbox  *memory.OutboxRepo
	deps    inboundmcp.Deps
	// tally is the facility-layout tally behind the deps, so tests can seed
	// storage positions / stations the way the facility consumer would.
	tally *memory.StorageTallyRepo
	flows *memory.ProcessFlowRepo
}

func newDeps() (inboundmcp.Deps, *memory.OutboxRepo) {
	deps, outboxRepo, _, _ := newStack()
	return deps, outboxRepo
}

func newStack() (inboundmcp.Deps, *memory.OutboxRepo, *memory.StorageTallyRepo, *memory.ProcessFlowRepo) {
	standards, tallyRepo := memory.NewStationStandardRepo(), memory.NewStorageTallyRepo()
	inboundStandards := memory.NewInboundStandardRepo()
	pc := memory.NewProcessCapacityRepo()
	flows := memory.NewProcessFlowRepo()
	plans, outboxRepo := memory.NewCapacityPlanRepo(), memory.NewOutboxRepo()
	uow := memory.NewUnitOfWork(pc, plans, outboxRepo)
	encoder := outboundkafka.NewEncoder()
	pathCapacity := &usecases.GetProcessPathCapacity{ProcessPaths: &usecases.FlowPathFinder{Flows: flows}, ProcessCapacities: pc, StationStandards: standards, Tally: tallyRepo}
	return inboundmcp.Deps{
		RegisterProcessCapacityConstraint: &usecases.RegisterProcessCapacityConstraint{Repo: pc},
		ProcessCapacities:                 pc,
		GetProcessPathCapacity:            pathCapacity,
		CreateCapacityPlan:                &usecases.CreateCapacityPlan{PathCapacity: pathCapacity, Plans: plans, Outbox: outboxRepo, Encoder: encoder, UnitOfWork: uow},
		PublishCapacityPlan:               &usecases.PublishCapacityPlan{Plans: plans, Outbox: outboxRepo, Encoder: encoder, UnitOfWork: uow},
		CapacityPlans:                     plans,
		DeclareStationStandard:            &usecases.DeclareStationStandard{Repo: standards},
		StationStandards:                  standards,
		GetStorageCapacity:                &usecases.GetStorageCapacity{Tally: tallyRepo},

		// ADR 0017: the inbound labour standard, inbound demand and plan
		// variance tools, wired as cmd/mcp wires them.
		DeclareInboundLabourStandard: &usecases.DeclareInboundLabourStandard{Repo: inboundStandards},
		GetInboundDemand:             &usecases.GetInboundDemand{Appointments: memory.NewInboundAppointmentRepo(), Standards: inboundStandards},
		GetCapacityPlanVariance:      &usecases.GetCapacityPlanVariance{Plans: plans, Observed: memory.NewObservedCapacityRepo()},
	}, outboxRepo, tallyRepo, flows
}

// seedPath records the ACTIVE flow a path id resolves to (ADR 0025 stage 3:
// flows are the only source); each stage's path is its own process type.
func (h *harness) seedPath(t *testing.T, flows *memory.ProcessFlowRepo, id string, steps []string) {
	t.Helper()
	stages := make([]processflow.Stage, 0, len(steps))
	for _, s := range steps {
		stages = append(stages, processflow.Stage{PathID: s, ProcessType: processflow.ProcessType(s)})
	}
	f, err := processflow.New(processflow.Params{FlowID: id, Name: id, Stages: stages, Status: processflow.StatusActive, Version: 1, UpdatedAt: time.Unix(0, 0).UTC()})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := flows.Record(context.Background(), f); err != nil {
		t.Fatal(err)
	}
}

func connectSession(t *testing.T, deps inboundmcp.Deps) *sdk.ClientSession {
	t.Helper()
	ctx := context.Background()
	clientTransport, serverTransport := sdk.NewInMemoryTransports()
	serverSession, err := inboundmcp.NewServer(deps).Connect(ctx, serverTransport, nil)
	if err != nil {
		t.Fatalf("server connect: %v", err)
	}
	t.Cleanup(func() { _ = serverSession.Close() })
	client := sdk.NewClient(&sdk.Implementation{Name: "test-client", Version: "0"}, nil)
	session, err := client.Connect(ctx, clientTransport, nil)
	if err != nil {
		t.Fatalf("client connect: %v", err)
	}
	t.Cleanup(func() { _ = session.Close() })
	return session
}

func newHarness(t *testing.T) *harness {
	t.Helper()
	deps, outboxRepo, tallyRepo, flows := newStack()
	return &harness{session: connectSession(t, deps), outbox: outboxRepo, deps: deps, tally: tallyRepo, flows: flows}
}

// call invokes a tool and fails the test on a TRANSPORT/protocol error;
// tool-level failures come back as res.IsError.
func (h *harness) call(t *testing.T, name string, args map[string]any) *sdk.CallToolResult {
	t.Helper()
	res, err := h.session.CallTool(context.Background(), &sdk.CallToolParams{Name: name, Arguments: args})
	if err != nil {
		t.Fatalf("%s: transport/protocol error (want a tool result): %v", name, err)
	}
	return res
}

// ok calls a tool, requires success, and returns its structured content.
func (h *harness) ok(t *testing.T, name string, args map[string]any) map[string]any {
	t.Helper()
	res := h.call(t, name, args)
	if res.IsError {
		t.Fatalf("%s returned a tool error: %s", name, text(res))
	}
	sc, isMap := res.StructuredContent.(map[string]any)
	if !isMap {
		t.Fatalf("%s: structured content = %#v, want an object", name, res.StructuredContent)
	}
	return sc
}

// fail calls a tool and requires an isError result whose text contains want.
func (h *harness) fail(t *testing.T, name string, args map[string]any, want string) {
	t.Helper()
	res := h.call(t, name, args)
	if !res.IsError {
		t.Fatalf("%s: expected an isError tool result, got %#v", name, res.StructuredContent)
	}
	if got := text(res); !strings.Contains(got, want) {
		t.Fatalf("%s: error text %q does not contain %q", name, got, want)
	}
}

func text(res *sdk.CallToolResult) string {
	var b strings.Builder
	for _, c := range res.Content {
		if tc, isText := c.(*sdk.TextContent); isText {
			b.WriteString(tc.Text)
		}
	}
	return b.String()
}

func constraintArgs(process, location string, quantity float64, unit string) map[string]any {
	return map[string]any{
		"process_type": process, "location": location, "window_start": winStart, "window_end": winEnd,
		"constraint_type": "LABOR", "quantity": quantity, "unit": unit, "period_seconds": 3600,
	}
}

// seedSection43 registers the design doc's section-43 capacities and path on
// location, through MCP tools only.
func (h *harness) seedSection43(t *testing.T, location, pathID string) {
	t.Helper()
	h.ok(t, "register_process_capacity_constraint", constraintArgs("PICK", location, 4000, "UNIT"))
	h.ok(t, "register_process_capacity_constraint", constraintArgs("REBIN", location, 2500, "UNIT"))
	h.ok(t, "register_process_capacity_constraint", constraintArgs("PACK", location, 1800, "PACKAGE"))
	h.seedPath(t, h.flows, pathID, []string{"PICK", "REBIN", "PACK"})
}

func planArgs(location, pathID string, demand float64) map[string]any {
	return map[string]any{
		"warehouse_id": "WH-1", "site_id": "SIM1", "location": location, "window_start": winStart, "window_end": winEnd,
		"path_id": pathID, "assigned_demand": demand, "units_per_order": 2.5, "packages_per_order": 1,
	}
}

func eventTypes(o *memory.OutboxRepo) []string {
	var got []string
	for _, m := range o.Messages() {
		got = append(got, strings.TrimPrefix(m.EventType, "com.warehouse.wes.warehouse-planning.capacityplan."))
	}
	return got
}

func asJSON(t *testing.T, v any) string {
	t.Helper()
	b, err := json.Marshal(v)
	if err != nil {
		t.Fatal(err)
	}
	return string(b)
}
