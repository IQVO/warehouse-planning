package main

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/a2aproject/a2a-go/v2/a2a"
	"github.com/a2aproject/a2a-go/v2/a2aclient"

	inbounda2a "github.com/claudioed/warehouse-planning/internal/adapters/inbound/a2a"
	"github.com/claudioed/warehouse-planning/internal/adapters/outbound/memory"
)

func quietLogger() *slog.Logger { return slog.New(slog.NewTextHandler(io.Discard, nil)) }

// There is no in-memory mode: an empty DATABASE_URL is a boot error naming it.
func TestBuildDeps_RefusesAnEmptyDatabaseURL(t *testing.T) {
	_, _, err := buildDeps(context.Background(), quietLogger(), "", "", defaultMigrationsPath)
	if !errors.Is(err, errMissingDatabaseURL) || !strings.Contains(err.Error(), "DATABASE_URL") {
		t.Fatalf("buildDeps(\"\") = %v, want errMissingDatabaseURL naming DATABASE_URL", err)
	}
}

// newDeps wires the SAME use cases as cmd/mcp over the repositories it is
// given: driven through the real router over the SDK client, the write skill
// reaches the shared outbox (the relay stays in cmd/api) and the read skill
// sees the same capacities.
func TestNewDeps_WiresBothSkillsOverTheSameStores(t *testing.T) {
	pc, plans, outboxRepo := memory.NewProcessCapacityRepo(), memory.NewCapacityPlanRepo(), memory.NewOutboxRepo()
	paths := memory.NewProcessPathRepo()
	deps, err := newDeps(repositories{
		processCapacities: pc, processPaths: paths, standards: memory.NewStationStandardRepo(),
		tally: memory.NewStorageTallyRepo(), plans: plans, outbox: outboxRepo, demand: memory.NewOrderDemandRepo(),
		uow:                 memory.NewUnitOfWork(pc, plans, outboxRepo),
		inboundAppointments: memory.NewInboundAppointmentRepo(), inboundStandards: memory.NewInboundStandardRepo(),
	})
	if err != nil {
		t.Fatalf("newDeps: %v", err)
	}
	if deps.GetProcessPathCapacity == nil || deps.CreateCapacityPlan == nil {
		t.Fatalf("deps = %+v, want both use cases wired", deps)
	}

	srv := httptest.NewServer(newRouter(inbounda2a.Handler(inbounda2a.NewExecutor(deps)),
		inbounda2a.Card(inbounda2a.Config{AgentURL: testAgentURL, Version: "test"})))
	t.Cleanup(srv.Close)
	client, err := a2aclient.NewFromCard(context.Background(), inbounda2a.Card(inbounda2a.Config{AgentURL: srv.URL + "/a2a", Version: "test"}))
	if err != nil {
		t.Fatal(err)
	}
	ask := func(skill, args string) string {
		t.Helper()
		part := a2a.NewTextPart(`{"skill":"` + skill + `","arguments":` + args + `}`)
		resp, err := client.SendMessage(context.Background(), &a2a.SendMessageRequest{Message: a2a.NewMessage(a2a.MessageRoleUser, part)})
		if err != nil {
			t.Fatalf("SendMessage: %v", err)
		}
		msg, ok := resp.(*a2a.Message)
		if !ok || len(msg.Parts) < 2 {
			t.Fatalf("reply = %T", resp)
		}
		text, _ := msg.Parts[1].Content.(a2a.Text)
		return string(text)
	}

	// Nothing registered: the read answers a 404 problem in-band.
	got := ask("path_capacity", `{"pathId":"p","location":"FC01","windowStart":"2026-10-05T08:00:00Z","windowEnd":"2026-10-05T16:00:00Z"}`)
	if !strings.Contains(got, "(404)") {
		t.Fatalf("read over empty stores = %q, want a 404 problem", got)
	}
	// A rejected write stores nothing in the shared outbox.
	got = ask("evaluate_capacity_plan", `{"warehouseId":"W","siteId":"S","location":"FC01","pathId":"p","windowStart":"2026-10-05T08:00:00Z","windowEnd":"2026-10-05T16:00:00Z","assignedDemand":1}`)
	if !strings.Contains(got, "(404)") || len(outboxRepo.Messages()) != 0 {
		t.Fatalf("write over empty stores = %q with %d outbox rows, want a 404 and no rows", got, len(outboxRepo.Messages()))
	}
}
