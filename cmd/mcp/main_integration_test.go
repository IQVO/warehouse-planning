//go:build integration

package main

import (
	"context"
	"path/filepath"
	"runtime"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	sdk "github.com/modelcontextprotocol/go-sdk/mcp"
	"github.com/testcontainers/testcontainers-go"
	tcpostgres "github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/wait"

	inboundmcp "github.com/claudioed/warehouse-planning/internal/adapters/inbound/mcp"
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

// The binary migrates a fresh testcontainers database, then the
// section-43 scenario run through MCP tools persists the plan AND its four
// outbox rows in Postgres (the relay in cmd/api would drain them; this
// binary starts none).
func TestMCPAgainstPostgres_Section43PersistsPlanAndOutbox(t *testing.T) {
	url := startPostgres(t)
	_, thisFile, _, _ := runtime.Caller(0)
	migrations := filepath.Join(filepath.Dir(thisFile), "..", "..", defaultMigrationsPath)

	ctx := context.Background()
	deps, closeFn, err := buildDeps(ctx, quietLogger(), url, url, migrations)
	if err != nil {
		t.Fatalf("buildDeps: %v", err)
	}
	t.Cleanup(closeFn)

	ct, st := sdk.NewInMemoryTransports()
	ss, err := inboundmcp.NewServer(deps).Connect(ctx, st, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer ss.Close()
	session, err := sdk.NewClient(&sdk.Implementation{Name: "t", Version: "0"}, nil).Connect(ctx, ct, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer session.Close()

	call := func(name string, args map[string]any) map[string]any {
		t.Helper()
		res, err := session.CallTool(ctx, &sdk.CallToolParams{Name: name, Arguments: args})
		if err != nil || res.IsError {
			t.Fatalf("%s: err=%v res=%+v", name, err, res)
		}
		return res.StructuredContent.(map[string]any)
	}
	const start, end = "2026-10-05T08:00:00Z", "2026-10-05T16:00:00Z"
	for _, c := range []struct {
		process string
		qty     float64
		unit    string
	}{{"PICK", 4000, "UNIT"}, {"REBIN", 2500, "UNIT"}, {"PACK", 1800, "PACKAGE"}} {
		call("register_process_capacity_constraint", map[string]any{
			"process_type": c.process, "location": "FC01", "window_start": start, "window_end": end,
			"constraint_type": "LABOR", "quantity": c.qty, "unit": c.unit, "period_seconds": 3600,
		})
	}
	call("register_process_path", map[string]any{"id": "tote-path", "name": "Tote", "steps": []string{"PICK", "REBIN", "PACK"}})
	plan := call("create_capacity_plan", map[string]any{
		"warehouse_id": "WH-1", "site_id": "FC01", "location": "FC01", "window_start": start, "window_end": end,
		"path_id": "tote-path", "assigned_demand": 12000, "units_per_order": 2.5, "packages_per_order": 1,
	})
	if plan["shortage"] != 4000.0 || plan["bottleneck_step"] != "REBIN" || plan["status"] != "DRAFT" {
		t.Fatalf("plan = %v", plan)
	}
	published := call("publish_capacity_plan", map[string]any{"id": plan["id"]})
	if published["status"] != "PUBLISHED" {
		t.Fatalf("published = %v", published)
	}
	res, err := session.CallTool(ctx, &sdk.CallToolParams{Name: "publish_capacity_plan", Arguments: map[string]any{"id": plan["id"]}})
	if err != nil || !res.IsError {
		t.Fatalf("second publish: err=%v res=%+v, want an isError result", err, res)
	}

	// Verify straight in Postgres (a fresh connection, i.e. after commit).
	conn, err := pgx.Connect(ctx, url)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close(ctx)
	var status string
	if err := conn.QueryRow(ctx, `SELECT status FROM capacity_plans WHERE id = $1`, plan["id"]).Scan(&status); err != nil || status != "PUBLISHED" {
		t.Fatalf("capacity_plans row: status=%q err=%v", status, err)
	}
	var outboxRows int
	if err := conn.QueryRow(ctx, `SELECT count(*) FROM outbox_events`).Scan(&outboxRows); err != nil || outboxRows != 8 {
		t.Fatalf("outbox_events rows = %d (err %v), want 8 (Created, Published, ShortageDetected, BottleneckDetected, each on the integration AND the analytics topic: ADR 0005)", outboxRows, err)
	}
	var analyticsRows int
	if err := conn.QueryRow(ctx, `SELECT count(*) FROM outbox_events WHERE topic = 'warehouse.warehouse-planning.analytics'`).Scan(&analyticsRows); err != nil || analyticsRows != 4 {
		t.Fatalf("analytics-topic rows = %d (err %v), want 4", analyticsRows, err)
	}
}
