//go:build integration

package main

import (
	"context"
	"encoding/json"
	"net/http/httptest"
	"path/filepath"
	"runtime"
	"testing"
	"time"

	"github.com/a2aproject/a2a-go/v2/a2a"
	"github.com/a2aproject/a2a-go/v2/a2aclient"
	"github.com/jackc/pgx/v5"
	"github.com/testcontainers/testcontainers-go"
	tcpostgres "github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/wait"

	inbounda2a "github.com/claudioed/warehouse-planning/internal/adapters/inbound/a2a"
	"github.com/claudioed/warehouse-planning/internal/adapters/outbound/postgres"
	"github.com/claudioed/warehouse-planning/internal/application/usecases"
	"github.com/claudioed/warehouse-planning/internal/domain/processcapacity"
	"github.com/claudioed/warehouse-planning/internal/domain/processpath"
)

func startPostgres(t *testing.T) string {
	t.Helper()
	ctx := context.Background()
	container, err := tcpostgres.Run(ctx, "postgres:16-alpine",
		tcpostgres.WithDatabase("warehouse_planning_test"),
		tcpostgres.WithUsername("warehouse_planning_test"),
		tcpostgres.WithPassword("warehouse_planning_test"),
		testcontainers.WithWaitStrategy(
			wait.ForLog("database system is ready to accept connections").WithOccurrence(2).WithStartupTimeout(60*time.Second),
		),
	)
	if err != nil {
		t.Fatalf("start postgres: %v", err)
	}
	t.Cleanup(func() { _ = container.Terminate(context.Background()) })
	url, err := container.ConnectionString(ctx, "sslmode=disable")
	if err != nil {
		t.Fatalf("connection string: %v", err)
	}
	return url
}

// seedSection43 registers the design doc's section-43 capacities and path
// straight through the use cases (the A2A agent deliberately offers no
// registration skill): PICK 4000 UNIT/h, REBIN 2500 UNIT/h, PACK 1800
// PACKAGE/h on PICK -> REBIN -> PACK.
func seedSection43(t *testing.T, url string) {
	t.Helper()
	ctx := context.Background()
	pool, err := postgres.NewPool(ctx, url)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	start, _ := time.Parse(time.RFC3339, "2026-10-05T08:00:00Z")
	end, _ := time.Parse(time.RFC3339, "2026-10-05T16:00:00Z")
	reg := &usecases.RegisterProcessCapacityConstraint{Repo: postgres.NewProcessCapacityRepo(pool)}
	for _, c := range []struct {
		process processcapacity.ProcessType
		qty     float64
		unit    processcapacity.CapacityUnit
	}{{"PICK", 4000, "UNIT"}, {"REBIN", 2500, "UNIT"}, {"PACK", 1800, "PACKAGE"}} {
		if _, err := reg.Handle(ctx, usecases.RegisterProcessCapacityConstraintCommand{
			ProcessType: c.process, Location: "FC01", WindowStart: start, WindowEnd: end,
			ConstraintType: "LABOR", Quantity: c.qty, Unit: c.unit, Period: time.Hour,
		}); err != nil {
			t.Fatalf("seed %s: %v", c.process, err)
		}
	}
	paths := &usecases.RegisterProcessPath{Repo: postgres.NewProcessPathRepo(pool)}
	if _, err := paths.Handle(ctx, usecases.RegisterProcessPathCommand{ID: "tote-path", Name: "Tote", Steps: []processpath.ProcessType{"PICK", "REBIN", "PACK"}}); err != nil {
		t.Fatal(err)
	}
}

// The delegated write goes through the SAME use case and transactional outbox
// as REST/MCP: against a fresh, migrated Postgres, evaluate_capacity_plan
// stores the DRAFT plan AND its CapacityPlanCreated CloudEvents in
// outbox_events in one transaction, all UNPUBLISHED (the relay lives in
// cmd/api, this binary starts none), and a rejected call stores nothing.
func TestA2AWriteSkillLeavesUnpublishedOutboxRows(t *testing.T) {
	url := startPostgres(t)
	_, thisFile, _, _ := runtime.Caller(0)
	migrations := filepath.Join(filepath.Dir(thisFile), "..", "..", defaultMigrationsPath)

	ctx := context.Background()
	deps, closeFn, err := buildDeps(ctx, quietLogger(), url, url, migrations)
	if err != nil {
		t.Fatalf("buildDeps: %v", err)
	}
	t.Cleanup(closeFn)
	seedSection43(t, url)

	srv := httptest.NewServer(newRouter(inbounda2a.Handler(inbounda2a.NewExecutor(deps)),
		inbounda2a.Card(inbounda2a.Config{AgentURL: testAgentURL, Version: "test"})))
	t.Cleanup(srv.Close)
	client, err := a2aclient.NewFromCard(ctx, inbounda2a.Card(inbounda2a.Config{AgentURL: srv.URL + "/a2a", Version: "test"}))
	if err != nil {
		t.Fatal(err)
	}
	send := func(skill string, args map[string]any) map[string]any {
		t.Helper()
		part := a2a.NewDataPart(map[string]any{"skill": skill, "arguments": args})
		resp, err := client.SendMessage(ctx, &a2a.SendMessageRequest{Message: a2a.NewMessage(a2a.MessageRoleUser, part)})
		if err != nil {
			t.Fatalf("SendMessage: %v", err)
		}
		msg, ok := resp.(*a2a.Message)
		if !ok || len(msg.Parts) == 0 {
			t.Fatalf("reply = %T", resp)
		}
		data, ok := msg.Parts[0].Content.(a2a.Data)
		if !ok {
			t.Fatalf("first reply part is %T, want data", msg.Parts[0].Content)
		}
		raw, _ := json.Marshal(data.Value)
		var env map[string]any
		if err := json.Unmarshal(raw, &env); err != nil {
			t.Fatal(err)
		}
		return env
	}
	window := map[string]any{"location": "FC01", "windowStart": "2026-10-05T08:00:00Z", "windowEnd": "2026-10-05T16:00:00Z", "unitsPerOrder": 2.5, "packagesPerOrder": 1}
	merge := func(extra map[string]any) map[string]any {
		out := map[string]any{}
		for k, v := range window {
			out[k] = v
		}
		for k, v := range extra {
			out[k] = v
		}
		return out
	}

	// The read skill sees the Postgres-backed capacities and writes nothing.
	read := send("path_capacity", merge(map[string]any{"pathId": "tote-path"}))
	if pc, _ := read["data"].(map[string]any)["pathCapacity"].(map[string]any); pc["normalizedRate"] != 1000.0 || pc["bottleneckStep"] != "REBIN" {
		t.Fatalf("path_capacity over Postgres = %v", read)
	}

	conn, err := pgx.Connect(ctx, url)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close(ctx)
	count := func(query string) int {
		t.Helper()
		var n int
		if err := conn.QueryRow(ctx, query).Scan(&n); err != nil {
			t.Fatalf("%s: %v", query, err)
		}
		return n
	}
	rowsBefore := count(`SELECT count(*) FROM outbox_events`)

	env := send("evaluate_capacity_plan", merge(map[string]any{"warehouseId": "WH-1", "siteId": "SIM1", "pathId": "tote-path", "assignedDemand": 12000}))
	plan, _ := env["data"].(map[string]any)["capacityPlan"].(map[string]any)
	if env["problem"] != nil || plan == nil || plan["status"] != "DRAFT" || plan["shortage"] != 4000.0 || plan["bottleneckStep"] != "REBIN" {
		t.Fatalf("evaluate_capacity_plan = %v", env)
	}

	var status string
	if err := conn.QueryRow(ctx, `SELECT status FROM capacity_plans WHERE id = $1`, plan["planId"]).Scan(&status); err != nil || status != "DRAFT" {
		t.Fatalf("capacity_plans row: status=%q err=%v", status, err)
	}
	// ProcessCapacityRegistered rows are not seeded here (no relay-bound
	// registration through A2A); the plan's own events are the delta.
	created := count(`SELECT count(*) FROM outbox_events WHERE event_type = 'com.warehouse.wes.warehouse-planning.capacityplan.CapacityPlanCreated'`)
	unpublished := count(`SELECT count(*) FROM outbox_events WHERE published_at IS NULL AND event_type = 'com.warehouse.wes.warehouse-planning.capacityplan.CapacityPlanCreated'`)
	if created != 2 || unpublished != created {
		t.Fatalf("CapacityPlanCreated rows = %d, unpublished = %d, want 2 (integration + analytics topic), all unpublished: the relay stays in cmd/api", created, unpublished)
	}
	if delta := count(`SELECT count(*) FROM outbox_events`) - rowsBefore; delta != 2 {
		t.Fatalf("the write added %d outbox rows, want exactly 2 (a DRAFT plan publishes nothing else)", delta)
	}
	if published := count(`SELECT count(*) FROM outbox_events WHERE published_at IS NOT NULL`); published != 0 {
		t.Fatalf("%d outbox rows are already published: this binary must not run the relay", published)
	}

	// A rejected call (unknown path) stores no plan and no outbox row.
	plansBefore, rows := count(`SELECT count(*) FROM capacity_plans`), count(`SELECT count(*) FROM outbox_events`)
	rejected := send("evaluate_capacity_plan", merge(map[string]any{"warehouseId": "WH-1", "siteId": "SIM1", "pathId": "ghost", "assignedDemand": 1}))
	if p, _ := rejected["problem"].(map[string]any); p == nil || p["status"] != 404.0 {
		t.Fatalf("rejected = %v, want an in-band 404 problem", rejected)
	}
	if count(`SELECT count(*) FROM capacity_plans`) != plansBefore || count(`SELECT count(*) FROM outbox_events`) != rows {
		t.Fatal("a rejected delegated write must store nothing")
	}
}
