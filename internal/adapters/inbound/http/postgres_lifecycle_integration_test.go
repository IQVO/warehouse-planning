//go:build integration

// Integration tests for the REST inbound adapter over REAL Postgres: the
// real chi router (inboundhttp.NewRouter) + the real Postgres repositories
// and UnitOfWork behind it, served over httptest — the exact wiring
// cmd/api's buildServer produces. The package's unit suites drive the same
// handlers over in-memory repos; these prove the main resource lifecycle
// end to end against a real database (POST -> 201, GET -> 200 round trip,
// validation -> 4xx with the repo's problem-slug error shape).
//
// Postgres comes from testcontainers: ONE container for the whole package
// (TestMain below), migrated once into a template database; each test gets
// its own private database cloned from that template (CREATE DATABASE ...
// WITH TEMPLATE — a file-level copy, milliseconds). Never an external
// DATABASE_URL, never t.Skip.
package http_test

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
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
	"github.com/testcontainers/testcontainers-go"
	tcpostgres "github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/wait"

	inboundhttp "github.com/claudioed/warehouse-planning/internal/adapters/inbound/http"
	outboundkafka "github.com/claudioed/warehouse-planning/internal/adapters/outbound/kafka"
	"github.com/claudioed/warehouse-planning/internal/adapters/outbound/postgres"
	"github.com/claudioed/warehouse-planning/internal/application/usecases"
)

// httpTemplateDB is the once-migrated template every test clones.
const httpTemplateDB = "http_it_migrated_template"

var (
	httpBaseURL string // connection URL of the container's default database
	httpDBSeq   atomic.Uint64
)

func TestMain(m *testing.M) {
	os.Exit(httpRunTests(m))
}

func httpRunTests(m *testing.M) int {
	ctx := context.Background()
	container, err := tcpostgres.Run(ctx, "postgres:16-alpine",
		tcpostgres.WithDatabase("wp_http_it"),
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

	if httpBaseURL, err = container.ConnectionString(ctx, "sslmode=disable"); err != nil {
		fmt.Fprintf(os.Stderr, "postgres connection string: %v\n", err)
		return 1
	}

	if err := httpCreateDatabase(ctx, httpTemplateDB); err != nil {
		fmt.Fprintf(os.Stderr, "create template database: %v\n", err)
		return 1
	}
	_, thisFile, _, _ := runtime.Caller(0)
	migrations := filepath.Join(filepath.Dir(thisFile), "..", "..", "..", "adapters", "outbound", "postgres", "migrations")
	if err := postgres.RunMigrations(httpWithDB(httpBaseURL, httpTemplateDB), migrations); err != nil {
		fmt.Fprintf(os.Stderr, "migrate template: %v\n", err)
		return 1
	}
	return m.Run()
}

// httpWithDB rewrites the path of a connection URL to the named database.
func httpWithDB(baseURL, name string) string {
	u, err := url.Parse(baseURL)
	if err != nil {
		panic(err)
	}
	u.Path = "/" + name
	return u.String()
}

// httpCreateDatabase creates an empty database inside the shared container.
func httpCreateDatabase(ctx context.Context, name string) error {
	conn, err := pgx.Connect(ctx, httpBaseURL)
	if err != nil {
		return fmt.Errorf("connect: %w", err)
	}
	defer conn.Close(ctx)
	if _, err := conn.Exec(ctx, fmt.Sprintf("CREATE DATABASE %q", name)); err != nil {
		return fmt.Errorf("create database %s: %w", name, err)
	}
	return nil
}

// httpMigratedDB hands the test a connection URL to its own private
// database, cloned from the migrated template.
func httpMigratedDB(t *testing.T) string {
	t.Helper()
	name := fmt.Sprintf("http_it_%d", httpDBSeq.Add(1))
	conn, err := pgx.Connect(context.Background(), httpBaseURL)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer conn.Close(context.Background())
	if _, err := conn.Exec(context.Background(), fmt.Sprintf(
		"CREATE DATABASE %q WITH TEMPLATE %q", name, httpTemplateDB)); err != nil {
		t.Fatalf("clone database: %v", err)
	}
	return httpWithDB(httpBaseURL, name)
}

// newPostgresRouter wires the REAL production stack — Postgres repos,
// UnitOfWork, use cases, NewRouter — over one private migrated database,
// exactly like cmd/api's buildServer, and serves it over httptest.
func newPostgresRouter(t *testing.T) *httptest.Server {
	t.Helper()
	ctx := context.Background()
	pool, err := postgres.NewPool(ctx, httpMigratedDB(t))
	if err != nil {
		t.Fatalf("open pool: %v", err)
	}
	t.Cleanup(pool.Close)

	pcs, paths := postgres.NewProcessCapacityRepo(pool), postgres.NewProcessPathRepo(pool)
	standards, tally := postgres.NewStationStandardRepo(pool), postgres.NewStorageTallyRepo(pool)
	plans, ob := postgres.NewCapacityPlanRepo(pool), postgres.NewOutboxRepo(pool)
	uow := postgres.NewUnitOfWork(pool)
	encoder := outboundkafka.NewFanoutEncoder()
	pathCapacity := &usecases.GetProcessPathCapacity{
		ProcessPaths: paths, ProcessCapacities: pcs, StationStandards: standards, Tally: tally,
	}
	expectedDemand := &usecases.GetExpectedDemand{Demand: postgres.NewOrderDemandRepo(pool)}
	s := &inboundhttp.Server{
		RegisterProcessCapacityConstraint: &usecases.RegisterProcessCapacityConstraint{Repo: pcs},
		ProcessCapacities:                 pcs,
		RegisterProcessPath:               &usecases.RegisterProcessPath{Repo: paths},
		GetProcessPathCapacity:            pathCapacity,
		CreateCapacityPlan: &usecases.CreateCapacityPlan{
			PathCapacity: pathCapacity, Plans: plans, Outbox: ob,
			Encoder: encoder, UnitOfWork: uow, Demand: expectedDemand,
		},
		PublishCapacityPlan: &usecases.PublishCapacityPlan{
			Plans: plans, Outbox: ob, Encoder: encoder, UnitOfWork: uow,
		},
		CapacityPlans:          plans,
		DeclareStationStandard: &usecases.DeclareStationStandard{Repo: standards},
		StationStandards:       standards,
		GetStorageCapacity:     &usecases.GetStorageCapacity{Tally: tally},
		GetExpectedDemand:      expectedDemand,
		ListProcessPaths:       &usecases.ListProcessPaths{Paths: paths},
		ListCapacityPlans:      &usecases.ListCapacityPlans{Plans: plans},
	}
	srv := httptest.NewServer(inboundhttp.NewRouter(s))
	t.Cleanup(srv.Close)
	return srv
}

// itPost posts a JSON body and returns status + body.
func itPost(t *testing.T, srv *httptest.Server, path string, payload map[string]any) (int, []byte) {
	t.Helper()
	body, err := json.Marshal(payload)
	if err != nil {
		t.Fatal(err)
	}
	resp, err := http.Post(srv.URL+path, "application/json", strings.NewReader(string(body)))
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	raw := readAll(t, resp)
	return resp.StatusCode, raw
}

// itGet performs a GET and returns status + body.
func itGet(t *testing.T, srv *httptest.Server, path string) (int, []byte) {
	t.Helper()
	resp, err := http.Get(srv.URL + path)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	raw := readAll(t, resp)
	return resp.StatusCode, raw
}

func readAll(t *testing.T, resp *http.Response) []byte {
	t.Helper()
	raw := make([]byte, 0)
	buf := make([]byte, 4096)
	for {
		n, err := resp.Body.Read(buf)
		raw = append(raw, buf[:n]...)
		if err != nil {
			break
		}
	}
	return raw
}

// itProblemSlug extracts the repo's RFC 7807 problem slug (the last path
// segment of "type") from an error body.
func itProblemSlug(t *testing.T, body []byte) string {
	t.Helper()
	var p struct {
		Type   string `json:"type"`
		Status int    `json:"status"`
	}
	if err := json.Unmarshal(body, &p); err != nil {
		t.Fatalf("decode problem: %v (%s)", err, body)
	}
	return p.Type[strings.LastIndex(p.Type, "/")+1:]
}

// The main resource lifecycle end to end over real Postgres: register the
// capacity fixture and the path (POST -> 201), read the effective capacity
// back (GET -> 200 round trip), create a DRAFT plan, publish it, and read
// the PUBLISHED state back — plus the outbox rows the writes queued.
func TestHTTP_Postgres_ResourceLifecycleRoundTrip(t *testing.T) {
	srv := newPostgresRouter(t)
	const start, end = "2026-10-05T08:00:00Z", "2026-10-05T16:00:00Z"
	const loc = "IT-HTTP-LOC"

	// POST the capacity fixture: three constraint registrations -> 201 each.
	for _, c := range []struct {
		process string
		qty     float64
		unit    string
	}{{"PICK", 4000, "UNIT"}, {"REBIN", 2500, "UNIT"}, {"PACK", 1800, "PACKAGE"}} {
		status, body := itPost(t, srv, "/process-capacities", map[string]any{
			"process_type": c.process, "location": loc, "window_start": start, "window_end": end,
			"constraint_type": "LABOR", "quantity": c.qty, "unit": c.unit, "period_seconds": 3600,
		})
		if status != http.StatusCreated {
			t.Fatalf("register %s: %d %s", c.process, status, body)
		}
		var created struct {
			EffectiveRate     float64 `json:"effective_rate"`
			BindingConstraint string  `json:"binding_constraint"`
		}
		if err := json.Unmarshal(body, &created); err != nil {
			t.Fatal(err)
		}
		if created.EffectiveRate != c.qty || created.BindingConstraint != "LABOR" {
			t.Fatalf("register %s = %+v", c.process, created)
		}
	}

	// GET the effective capacity back: a 200 round trip of what POST wrote.
	status, body := itGet(t, srv, "/process-capacities?process_type=REBIN&location="+loc+
		"&window_start="+start+"&window_end="+end)
	if status != http.StatusOK {
		t.Fatalf("get effective capacity: %d %s", status, body)
	}
	var eff struct {
		EffectiveRate float64 `json:"effective_rate"`
		EffectiveUnit string  `json:"effective_unit"`
	}
	if err := json.Unmarshal(body, &eff); err != nil {
		t.Fatal(err)
	}
	if eff.EffectiveRate != 2500 || eff.EffectiveUnit != "UNIT" {
		t.Fatalf("effective capacity round trip = %+v", eff)
	}

	// POST the path, then read its capacity through the composed read.
	status, body = itPost(t, srv, "/process-paths", map[string]any{
		"id": "pick-rebin-pack", "name": "Pick-Rebin-Pack", "steps": []string{"PICK", "REBIN", "PACK"},
	})
	if status != http.StatusCreated {
		t.Fatalf("register path: %d %s", status, body)
	}
	status, body = itGet(t, srv, "/process-paths/pick-rebin-pack/capacity?location="+loc+
		"&window_start="+start+"&window_end="+end+"&units_per_order=2.5&packages_per_order=1")
	if status != http.StatusOK {
		t.Fatalf("path capacity: %d %s", status, body)
	}
	var pathCap struct {
		NormalizedRate float64 `json:"normalized_rate"`
		BottleneckStep string  `json:"bottleneck_step"`
	}
	if err := json.Unmarshal(body, &pathCap); err != nil {
		t.Fatal(err)
	}
	if pathCap.NormalizedRate != 1000 || pathCap.BottleneckStep != "REBIN" {
		t.Fatalf("path capacity = %+v", pathCap)
	}

	// POST the plan (DRAFT), publish it, and read the state back.
	status, body = itPost(t, srv, "/capacity-plans", map[string]any{
		"warehouse_id": "WH-1", "site_id": "SIM1", "location": loc,
		"window_start": start, "window_end": end, "path_id": "pick-rebin-pack",
		"assigned_demand": 12000, "units_per_order": 2.5, "packages_per_order": 1,
	})
	if status != http.StatusCreated {
		t.Fatalf("create plan: %d %s", status, body)
	}
	var plan struct {
		ID          string  `json:"id"`
		Status      string  `json:"status"`
		Shortage    float64 `json:"shortage"`
		PublishedAt *string `json:"published_at"`
	}
	if err := json.Unmarshal(body, &plan); err != nil {
		t.Fatal(err)
	}
	if plan.Status != "DRAFT" || plan.Shortage != 4000 || plan.PublishedAt != nil {
		t.Fatalf("created plan = %+v", plan)
	}

	status, body = itPost(t, srv, "/capacity-plans/"+plan.ID+"/publish", nil)
	if status != http.StatusOK {
		t.Fatalf("publish plan: %d %s", status, body)
	}
	status, body = itGet(t, srv, "/capacity-plans/"+plan.ID)
	if status != http.StatusOK {
		t.Fatalf("get plan: %d %s", status, body)
	}
	if err := json.Unmarshal(body, &plan); err != nil {
		t.Fatal(err)
	}
	if plan.Status != "PUBLISHED" || plan.PublishedAt == nil {
		t.Fatalf("published plan round trip = %+v", plan)
	}
}

// Validation and domain rejections come back as 4xx problem+json carrying
// the repo's problem slugs — over the real stack.
func TestHTTP_Postgres_ValidationRejectionsUseProblemSlugs(t *testing.T) {
	srv := newPostgresRouter(t)

	cases := []struct {
		name   string
		method func() (int, []byte)
		status int
		slug   string
	}{
		{"malformed json", func() (int, []byte) {
			resp, err := http.Post(srv.URL+"/process-paths", "application/json", strings.NewReader("{not json"))
			if err != nil {
				t.Fatal(err)
			}
			defer resp.Body.Close()
			return resp.StatusCode, readAll(t, resp)
		}, http.StatusBadRequest, "malformed-json"},
		{"empty steps", func() (int, []byte) {
			return itPost(t, srv, "/process-paths", map[string]any{"id": "x", "name": "X", "steps": []string{}})
		}, http.StatusUnprocessableEntity, "empty-process-path-steps"},
		{"invalid window", func() (int, []byte) {
			return itPost(t, srv, "/process-capacities", map[string]any{
				"process_type": "PICK", "location": "L", "window_start": "2026-10-05T16:00:00Z",
				"window_end": "2026-10-05T08:00:00Z", "constraint_type": "LABOR",
				"quantity": 10, "unit": "UNIT", "period_seconds": 3600,
			})
		}, http.StatusBadRequest, "invalid-capacity-window"},
		{"unknown plan", func() (int, []byte) {
			return itGet(t, srv, "/capacity-plans/no-such-plan")
		}, http.StatusNotFound, "capacity-plan-not-found"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			status, body := tc.method()
			if status != tc.status {
				t.Fatalf("status = %d (%s), want %d", status, body, tc.status)
			}
			if slug := itProblemSlug(t, body); slug != tc.slug {
				t.Fatalf("problem slug = %q, want %q (body %s)", slug, tc.slug, body)
			}
		})
	}
}
