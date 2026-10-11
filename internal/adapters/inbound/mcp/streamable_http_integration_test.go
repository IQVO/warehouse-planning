//go:build integration

// Integration tests for the MCP inbound adapter over the REAL Streamable
// HTTP transport: mcp.Handler(server) mounted on an httptest.Server, driven
// by the SDK's own client (mcp.NewClient + StreamableClientTransport), with
// the REAL Postgres-backed use cases behind it — exactly the deployment
// shape cmd/mcp serves. This proves the wire contract (initialize,
// tools/list, tools/call) end-to-end against real infrastructure; the
// in-memory suites in this package prove the tool handlers in isolation.
//
// Postgres comes from testcontainers: ONE container for the whole package
// (TestMain below), migrated once into a template database; each test then
// gets its own private database cloned from that template (CREATE DATABASE
// ... WITH TEMPLATE — a file-level copy, milliseconds). Never an external
// DATABASE_URL, never t.Skip.
package mcp_test

import (
	"context"
	"fmt"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	sdkmcp "github.com/modelcontextprotocol/go-sdk/mcp"
	"github.com/testcontainers/testcontainers-go"
	tcpostgres "github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/wait"

	mcpadapter "github.com/claudioed/warehouse-planning/internal/adapters/inbound/mcp"
	outboundkafka "github.com/claudioed/warehouse-planning/internal/adapters/outbound/kafka"
	"github.com/claudioed/warehouse-planning/internal/adapters/outbound/postgres"
	"github.com/claudioed/warehouse-planning/internal/application/usecases"
)

// itTemplateDB is the once-migrated template every test clones, so the
// (slow) migration step runs exactly once per package run.
const itTemplateDB = "mcp_it_migrated_template"

var (
	itSharedBaseURL string // connection URL of the container's default database
	itDBSeq         atomic.Uint64
)

func TestMain(m *testing.M) {
	os.Exit(itRunTests(m))
}

func itRunTests(m *testing.M) int {
	ctx := context.Background()
	container, err := tcpostgres.Run(ctx, "postgres:16-alpine",
		tcpostgres.WithDatabase("wp_mcp_it"),
		tcpostgres.WithUsername("wp"),
		tcpostgres.WithPassword("wp"),
		testcontainers.WithWaitStrategy(
			wait.ForLog("database system is ready to accept connections").
				WithOccurrence(2).
				WithStartupTimeout(90*time.Second),
		),
	)
	if err != nil {
		fmt.Fprintf(os.Stderr, "start postgres container: %v\n", err)
		return 1
	}
	defer func() {
		if err := testcontainers.TerminateContainer(container); err != nil {
			fmt.Fprintf(os.Stderr, "terminate postgres container: %v\n", err)
		}
	}()

	if itSharedBaseURL, err = container.ConnectionString(ctx, "sslmode=disable"); err != nil {
		fmt.Fprintf(os.Stderr, "postgres connection string: %v\n", err)
		return 1
	}

	// Migrate a template database once; every test clones it.
	if err := itCreateDatabase(ctx, itTemplateDB); err != nil {
		fmt.Fprintf(os.Stderr, "create template database: %v\n", err)
		return 1
	}
	_, thisFile, _, _ := runtime.Caller(0)
	migrations := filepath.Join(filepath.Dir(thisFile), "..", "..", "outbound", "postgres", "migrations")
	if err := postgres.RunMigrations(itWithDB(itSharedBaseURL, itTemplateDB), migrations); err != nil {
		fmt.Fprintf(os.Stderr, "migrate template: %v\n", err)
		return 1
	}
	return m.Run()
}

// itWithDB rewrites the path of a connection URL to the named database.
func itWithDB(baseURL, name string) string {
	u, err := url.Parse(baseURL)
	if err != nil {
		panic(err)
	}
	u.Path = "/" + name
	return u.String()
}

// itCreateDatabase creates an empty database inside the shared container.
func itCreateDatabase(ctx context.Context, name string) error {
	conn, err := pgx.Connect(ctx, itSharedBaseURL)
	if err != nil {
		return fmt.Errorf("connect: %w", err)
	}
	defer conn.Close(ctx)
	if _, err := conn.Exec(ctx, fmt.Sprintf("CREATE DATABASE %q", name)); err != nil {
		return fmt.Errorf("create database %s: %w", name, err)
	}
	return nil
}

// itMigratedDB hands the test a connection URL to its own private database,
// cloned from the migrated template.
func itMigratedDB(t *testing.T) string {
	t.Helper()
	name := fmt.Sprintf("mcp_it_%d", itDBSeq.Add(1))
	conn, err := pgx.Connect(context.Background(), itSharedBaseURL)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer conn.Close(context.Background())
	if _, err := conn.Exec(context.Background(), fmt.Sprintf(
		"CREATE DATABASE %q WITH TEMPLATE %q", name, itTemplateDB)); err != nil {
		t.Fatalf("clone database: %v", err)
	}
	return itWithDB(itSharedBaseURL, name)
}

// itHarness is a connected SDK client session over the REAL production
// stack — Postgres repos, UnitOfWork, use cases, mcp.NewServer,
// mcp.Handler — served over Streamable HTTP, plus the private database URL
// so tests can assert on what actually persisted.
type itHarness struct {
	session *sdkmcp.ClientSession
	dbURL   string
}

// newITHarness wires the real stack exactly like cmd/mcp's buildDeps does
// for a configured DATABASE_URL, and serves it over httptest.
func newITHarness(t *testing.T) *itHarness {
	t.Helper()
	ctx := context.Background()
	dbURL := itMigratedDB(t)
	pool, err := postgres.NewPool(ctx, dbURL)
	if err != nil {
		t.Fatalf("open pool: %v", err)
	}
	t.Cleanup(pool.Close)

	pcRepo, pathRepo := postgres.NewProcessCapacityRepo(pool), postgres.NewProcessPathRepo(pool)
	standards, tally := postgres.NewStationStandardRepo(pool), postgres.NewStorageTallyRepo(pool)
	planRepo, ob := postgres.NewCapacityPlanRepo(pool), postgres.NewOutboxRepo(pool)
	demandRepo := postgres.NewOrderDemandRepo(pool)
	uow := postgres.NewUnitOfWork(pool)
	encoder := outboundkafka.NewFanoutEncoder() // ADR 0005: events + analytics

	pathCapacity := &usecases.GetProcessPathCapacity{
		ProcessPaths: pathRepo, ProcessCapacities: pcRepo, StationStandards: standards, Tally: tally,
	}
	expectedDemand := &usecases.GetExpectedDemand{Demand: demandRepo}
	deps := mcpadapter.Deps{
		RegisterProcessCapacityConstraint: &usecases.RegisterProcessCapacityConstraint{Repo: pcRepo},
		ProcessCapacities:                 pcRepo,
		RegisterProcessPath:               &usecases.RegisterProcessPath{Repo: pathRepo},
		GetProcessPathCapacity:            pathCapacity,
		CreateCapacityPlan: &usecases.CreateCapacityPlan{
			PathCapacity: pathCapacity, Plans: planRepo, Outbox: ob,
			Encoder: encoder, UnitOfWork: uow, Demand: expectedDemand,
		},
		PublishCapacityPlan: &usecases.PublishCapacityPlan{
			Plans: planRepo, Outbox: ob, Encoder: encoder, UnitOfWork: uow,
		},
		CapacityPlans:          planRepo,
		DeclareStationStandard: &usecases.DeclareStationStandard{Repo: standards},
		StationStandards:       standards,
		GetStorageCapacity:     &usecases.GetStorageCapacity{Tally: tally},
		GetExpectedDemand:      expectedDemand,
	}

	hs := httptest.NewServer(mcpadapter.Handler(mcpadapter.NewServer(deps)))
	t.Cleanup(hs.Close)

	client := sdkmcp.NewClient(&sdkmcp.Implementation{Name: "itcov-test-host", Version: "1.0.0"}, nil)
	session, err := client.Connect(ctx, &sdkmcp.StreamableClientTransport{
		Endpoint: hs.URL, DisableStandaloneSSE: true,
	}, nil)
	if err != nil {
		t.Fatalf("mcp connect: %v", err)
	}
	t.Cleanup(func() { _ = session.Close() })
	return &itHarness{session: session, dbURL: dbURL}
}

// itCall invokes a tool and fails the test on a TRANSPORT/protocol error;
// tool-level failures come back as res.IsError.
func (h *itHarness) itCall(t *testing.T, name string, args map[string]any) *sdkmcp.CallToolResult {
	t.Helper()
	res, err := h.session.CallTool(context.Background(), &sdkmcp.CallToolParams{Name: name, Arguments: args})
	if err != nil {
		t.Fatalf("%s: transport/protocol error (want a tool result): %v", name, err)
	}
	return res
}

// itOK calls a tool, requires success, and returns its structured content.
func (h *itHarness) itOK(t *testing.T, name string, args map[string]any) map[string]any {
	t.Helper()
	res := h.itCall(t, name, args)
	if res.IsError {
		t.Fatalf("%s returned a tool error: %s", name, itToolText(res))
	}
	sc, isMap := res.StructuredContent.(map[string]any)
	if !isMap {
		t.Fatalf("%s: structured content = %#v, want an object", name, res.StructuredContent)
	}
	return sc
}

// itToolText concatenates a tool result's text content.
func itToolText(res *sdkmcp.CallToolResult) string {
	var b strings.Builder
	for _, c := range res.Content {
		if tc, isText := c.(*sdkmcp.TextContent); isText {
			b.WriteString(tc.Text)
		}
	}
	return b.String()
}

// The MCP governance charter's curated surface, with each tool's read-only
// annotation, as served over the real transport.
func TestMCPStreamableHTTP_ListToolsExposesTheContract(t *testing.T) {
	h := newITHarness(t)
	list, err := h.session.ListTools(context.Background(), nil)
	if err != nil {
		t.Fatalf("tools/list: %v", err)
	}
	want := map[string]bool{ // name -> read-only
		"register_process_capacity_constraint": false,
		"get_effective_process_capacity":       true,
		"register_process_path":                false,
		"get_process_path_capacity":            true,
		"create_capacity_plan":                 false,
		"publish_capacity_plan":                false,
		"get_capacity_plan":                    true,
		"declare_station_standard":             false,
		"list_station_standards":               true,
		"get_storage_capacity":                 true,
		"get_expected_demand":                  true,
	}
	got := map[string]bool{}
	for _, tool := range list.Tools {
		got[tool.Name] = true
		if wantReadOnly, known := want[tool.Name]; known {
			if tool.Annotations == nil || tool.Annotations.ReadOnlyHint != wantReadOnly {
				t.Errorf("%s: ReadOnlyHint = %v, want %v", tool.Name, tool.Annotations, wantReadOnly)
			}
		}
	}
	if len(got) != len(want) {
		t.Fatalf("tools/list exposed %d tools %v, want exactly the %d curated ones", len(got), got, len(want))
	}
	for name := range want {
		if !got[name] {
			t.Errorf("tools/list must expose %q", name)
		}
	}
}

// The section-43 lifecycle driven purely through tools/call over Streamable
// HTTP against real Postgres: write tools persist, read tools read back,
// and the plan AND its outbox rows really land in the database.
func TestMCPStreamableHTTP_CallToolRoundTripThroughPostgres(t *testing.T) {
	h := newITHarness(t)
	const start, end = "2026-10-05T08:00:00Z", "2026-10-05T16:00:00Z"

	// Write tool: register the PICK capacity.
	reg := h.itOK(t, "register_process_capacity_constraint", map[string]any{
		"process_type": "PICK", "location": "IT-MCP-LOC", "window_start": start, "window_end": end,
		"constraint_type": "LABOR", "quantity": 4000, "unit": "UNIT", "period_seconds": 3600,
	})
	if reg["effective_rate"] != 4000.0 || reg["binding_constraint"] != "LABOR" {
		t.Fatalf("register constraint = %v", reg)
	}

	// Read tool: the effective capacity reads the SAME row back.
	eff := h.itOK(t, "get_effective_process_capacity", map[string]any{
		"process_type": "PICK", "location": "IT-MCP-LOC", "window_start": start, "window_end": end,
	})
	if eff["effective_rate"] != 4000.0 || eff["effective_unit"] != "UNIT" {
		t.Fatalf("effective capacity = %v", eff)
	}

	// The path and the shortage plan.
	h.itOK(t, "register_process_path", map[string]any{
		"id": "it-mcp-path", "name": "It MCP path", "steps": []string{"PICK"},
	})
	plan := h.itOK(t, "create_capacity_plan", map[string]any{
		"warehouse_id": "WH-1", "site_id": "SIM1", "location": "IT-MCP-LOC",
		"window_start": start, "window_end": end, "path_id": "it-mcp-path",
		"assigned_demand": 100, // PICK alone: 4000 UNIT/h / 1 unit per order = 4000 ORDER/h, no shortage
		"units_per_order": 1, "packages_per_order": 1,
	})
	if plan["status"] != "DRAFT" || plan["shortage"] != 0.0 {
		t.Fatalf("plan = %v", plan)
	}
	planID, _ := plan["id"].(string)
	if planID == "" {
		t.Fatalf("create_capacity_plan returned no id: %v", plan)
	}

	published := h.itOK(t, "publish_capacity_plan", map[string]any{"id": planID})
	if published["status"] != "PUBLISHED" {
		t.Fatalf("published = %v", published)
	}
	readBack := h.itOK(t, "get_capacity_plan", map[string]any{"id": planID})
	if readBack["status"] != "PUBLISHED" || readBack["published_at"] == nil {
		t.Fatalf("get_capacity_plan = %v", readBack)
	}

	// A second publish is a DOMAIN rejection and must surface as a tool
	// error carrying the slug, never a transport error.
	second := h.itCall(t, "publish_capacity_plan", map[string]any{"id": planID})
	if !second.IsError || !strings.Contains(itToolText(second), "capacity-plan-already-published") {
		t.Fatalf("second publish = isError %v, text %q; want a capacity-plan-already-published tool error", second.IsError, itToolText(second))
	}

	// It all really persisted: the plan row is PUBLISHED and the outbox
	// holds Created + Published on BOTH streams (ADR 0005 fanout).
	ctx := context.Background()
	conn, err := pgx.Connect(ctx, h.dbURL)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer conn.Close(ctx)
	var status string
	if err := conn.QueryRow(ctx, `SELECT status FROM capacity_plans WHERE id = $1`, planID).Scan(&status); err != nil || status != "PUBLISHED" {
		t.Fatalf("capacity_plans row: status=%q err=%v", status, err)
	}
	for topic, wantRows := range map[string]int{
		"warehouse.warehouse-planning.events":    2,
		"warehouse.warehouse-planning.analytics": 2,
	} {
		var n int
		if err := conn.QueryRow(ctx, `SELECT count(*) FROM outbox_events WHERE topic = $1`, topic).Scan(&n); err != nil || n != wantRows {
			t.Fatalf("outbox rows on %s = %d (err %v), want %d", topic, n, err, wantRows)
		}
	}
}

// Invalid input and unknown identities are tool errors with the REST
// vocabulary's slugs — the adapter never lets them become transport
// failures.
func TestMCPStreamableHTTP_RejectionsAreToolErrors(t *testing.T) {
	h := newITHarness(t)

	res := h.itCall(t, "register_process_capacity_constraint", map[string]any{
		"process_type": "PICK", "location": "IT-MCP-LOC",
		"window_start": "not-a-timestamp", "window_end": "2026-10-05T16:00:00Z",
		"constraint_type": "LABOR", "quantity": 10, "unit": "UNIT", "period_seconds": 3600,
	})
	if !res.IsError || !strings.Contains(itToolText(res), "malformed-window-start") {
		t.Fatalf("malformed window = isError %v, text %q", res.IsError, itToolText(res))
	}

	res = h.itCall(t, "get_effective_process_capacity", map[string]any{
		"process_type": "PACK", "location": "NOWHERE",
		"window_start": "2026-10-05T08:00:00Z", "window_end": "2026-10-05T16:00:00Z",
	})
	if !res.IsError || !strings.Contains(itToolText(res), "process-capacity-not-found") {
		t.Fatalf("unknown identity = isError %v, text %q", res.IsError, itToolText(res))
	}

	res = h.itCall(t, "register_process_path", map[string]any{"id": "empty", "name": "Empty", "steps": []string{}})
	if !res.IsError || !strings.Contains(itToolText(res), "empty-process-path-steps") {
		t.Fatalf("empty steps = isError %v, text %q", res.IsError, itToolText(res))
	}

	res = h.itCall(t, "get_capacity_plan", map[string]any{"id": "no-such-plan"})
	if !res.IsError || !strings.Contains(itToolText(res), "capacity-plan-not-found") {
		t.Fatalf("unknown plan = isError %v, text %q", res.IsError, itToolText(res))
	}
}
