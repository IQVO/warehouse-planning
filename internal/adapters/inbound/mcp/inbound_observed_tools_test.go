package mcp_test

import (
	"testing"
	"time"

	sdk "github.com/modelcontextprotocol/go-sdk/mcp"

	inboundmcp "github.com/claudioed/warehouse-planning/internal/adapters/inbound/mcp"
	"github.com/claudioed/warehouse-planning/internal/adapters/outbound/memory"
	"github.com/claudioed/warehouse-planning/internal/application/usecases"
	"github.com/claudioed/warehouse-planning/internal/domain/inbound"
)

func toolNames(t *testing.T, session *sdk.ClientSession) map[string]bool {
	t.Helper()
	res, err := session.ListTools(t.Context(), nil)
	if err != nil {
		t.Fatalf("ListTools: %v", err)
	}
	names := map[string]bool{}
	for _, tool := range res.Tools {
		names[tool.Name] = true
	}
	return names
}

func inboundObservedDeps() (inboundmcp.Deps, *memory.InboundAppointmentRepo) {
	deps, _, _ := newStack()
	appointments, standards := memory.NewInboundAppointmentRepo(), memory.NewInboundStandardRepo()
	deps.DeclareInboundLabourStandard = &usecases.DeclareInboundLabourStandard{Repo: standards}
	deps.GetInboundDemand = &usecases.GetInboundDemand{Appointments: appointments, Standards: standards}
	deps.GetCapacityPlanVariance = &usecases.GetCapacityPlanVariance{Plans: memory.NewCapacityPlanRepo(), Observed: memory.NewObservedCapacityRepo()}
	return deps, appointments
}

func TestInboundObservedTools_ListedOnlyWhenWired(t *testing.T) {
	deps, _ := inboundObservedDeps()
	wired := toolNames(t, connectSession(t, deps))
	for _, want := range []string{"declare_inbound_labour_standard", "get_inbound_demand", "get_capacity_plan_variance"} {
		if !wired[want] {
			t.Errorf("tool %s not advertised when wired", want)
		}
	}
	bare, _, _ := newStack()
	bare.DeclareInboundLabourStandard, bare.GetInboundDemand, bare.GetCapacityPlanVariance = nil, nil, nil
	unwired := toolNames(t, connectSession(t, bare))
	for _, name := range []string{"declare_inbound_labour_standard", "get_inbound_demand", "get_capacity_plan_variance"} {
		if unwired[name] {
			t.Errorf("tool %s advertised without its use case", name)
		}
	}
}

func TestInboundDemandTool_PricedByDeclaredStandard(t *testing.T) {
	deps, appointments := inboundObservedDeps()
	h := &harness{session: connectSession(t, deps), deps: deps}
	start := time.Date(2026, 10, 5, 8, 0, 0, 0, time.UTC)
	booked, err := inbound.NewBooked(inbound.BookedParams{
		AppointmentID: "A1", Location: "SIM1", WindowStart: start.Add(time.Hour), WindowEnd: start.Add(2 * time.Hour),
		ASNCount: 3, AsOf: start,
	})
	if err != nil {
		t.Fatalf("NewBooked: %v", err)
	}
	if _, err := appointments.Apply(t.Context(), booked); err != nil {
		t.Fatalf("Apply: %v", err)
	}
	args := map[string]any{"location": "SIM1", "path_id": "receive", "window_start": winStart, "window_end": winEnd}

	got := h.ok(t, "get_inbound_demand", args)
	if got["appointments"] != 1.0 || got["labour_minutes"] != 0.0 || got["standard_missing"] != true {
		t.Fatalf("without a standard: %v", got)
	}

	declared := h.ok(t, "declare_inbound_labour_standard", map[string]any{
		"location": "SIM1", "path_id": "receive", "minutes_per_appointment": 30, "minutes_per_asn": 10,
	})
	if declared["created"] != true {
		t.Fatalf("declare = %v, want created", declared)
	}
	got = h.ok(t, "get_inbound_demand", args)
	// 30 + 3 ASNs * 10 = 60 minutes.
	if got["labour_minutes"] != 60.0 || got["asns"] != 3.0 || got["standard_declared"] != true || got["standard_missing"] != false || got["source"] != "inbound-receiving" {
		t.Fatalf("with a standard: %v", got)
	}
}

func TestInboundObservedTools_RejectBadInput(t *testing.T) {
	deps, _ := inboundObservedDeps()
	h := &harness{session: connectSession(t, deps), deps: deps}
	h.fail(t, "declare_inbound_labour_standard", map[string]any{"location": "SIM1", "path_id": "receive", "minutes_per_appointment": 0}, "positive")
	h.fail(t, "get_inbound_demand", map[string]any{"location": "", "path_id": "receive", "window_start": winStart, "window_end": winEnd}, "location")
	h.fail(t, "get_inbound_demand", map[string]any{"location": "SIM1", "path_id": "", "window_start": winStart, "window_end": winEnd}, "path_id")
	h.fail(t, "get_capacity_plan_variance", map[string]any{"id": "missing"}, "")
}
