//go:build integration

// Package usecases_test proves this repo's main write use cases against a
// REAL Postgres (testcontainers): the real Postgres repositories, the real
// UnitOfWork, and the real CloudEvents encoders, wired exactly like the
// cmd/api composition root. These are integration tests in the fleet's
// sense: they execute the real cross-component contracts (capacity
// registration, path declaration, the plan aggregate's create -> publish
// lifecycle, the atomic save+outbox bracket inside one UnitOfWork) against
// real infrastructure, with no in-memory repo fakes anywhere in the path.
//
// The package boots its own throwaway Postgres via testcontainers in
// TestMain: one container for the whole package, migrated ONCE into a
// template database, one private database per test (CREATE DATABASE ...
// WITH TEMPLATE — a file-level copy, milliseconds). Never an external
// DATABASE_URL, never t.Skip.
package usecases_test

import (
	"context"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"runtime"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/testcontainers/testcontainers-go"
	tcpostgres "github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/wait"

	outboundkafka "github.com/claudioed/warehouse-planning/internal/adapters/outbound/kafka"
	"github.com/claudioed/warehouse-planning/internal/adapters/outbound/postgres"
	"github.com/claudioed/warehouse-planning/internal/application/usecases"
	"github.com/claudioed/warehouse-planning/internal/domain/capacityplan"
	"github.com/claudioed/warehouse-planning/internal/domain/processcapacity"
	"github.com/claudioed/warehouse-planning/internal/domain/processpath"
)

// wireTemplateDB is the once-migrated template every test clones.
const wireTemplateDB = "usecases_it_migrated_template"

var (
	wireBaseURL string // connection URL of the container's default database
	wireDBSeq   atomic.Uint64
)

func TestMain(m *testing.M) {
	os.Exit(wireRunTests(m))
}

func wireRunTests(m *testing.M) int {
	ctx := context.Background()
	container, err := tcpostgres.Run(ctx, "postgres:16-alpine",
		tcpostgres.WithDatabase("wp_usecases_it"),
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

	if wireBaseURL, err = container.ConnectionString(ctx, "sslmode=disable"); err != nil {
		fmt.Fprintf(os.Stderr, "postgres connection string: %v\n", err)
		return 1
	}

	if err := wireCreateDatabase(ctx, wireTemplateDB); err != nil {
		fmt.Fprintf(os.Stderr, "create template database: %v\n", err)
		return 1
	}
	_, thisFile, _, _ := runtime.Caller(0)
	migrations := filepath.Join(filepath.Dir(thisFile), "..", "..", "adapters", "outbound", "postgres", "migrations")
	if err := postgres.RunMigrations(wireWithDB(wireBaseURL, wireTemplateDB), migrations); err != nil {
		fmt.Fprintf(os.Stderr, "migrate template: %v\n", err)
		return 1
	}
	return m.Run()
}

// wireWithDB rewrites the path of a connection URL to the named database.
func wireWithDB(baseURL, name string) string {
	u, err := url.Parse(baseURL)
	if err != nil {
		panic(err)
	}
	u.Path = "/" + name
	return u.String()
}

// wireCreateDatabase creates an empty database inside the shared container.
func wireCreateDatabase(ctx context.Context, name string) error {
	conn, err := pgx.Connect(ctx, wireBaseURL)
	if err != nil {
		return fmt.Errorf("connect: %w", err)
	}
	defer conn.Close(ctx)
	if _, err := conn.Exec(ctx, fmt.Sprintf("CREATE DATABASE %q", name)); err != nil {
		return fmt.Errorf("create database %s: %w", name, err)
	}
	return nil
}

// wireMigratedDB hands the test a connection URL to its own private
// database, cloned from the migrated template.
func wireMigratedDB(t *testing.T) string {
	t.Helper()
	name := fmt.Sprintf("usecases_it_%d", wireDBSeq.Add(1))
	conn, err := pgx.Connect(context.Background(), wireBaseURL)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer conn.Close(context.Background())
	if _, err := conn.Exec(context.Background(), fmt.Sprintf(
		"CREATE DATABASE %q WITH TEMPLATE %q", name, wireTemplateDB)); err != nil {
		t.Fatalf("clone database: %v", err)
	}
	return wireWithDB(wireBaseURL, name)
}

// wiredStack is the real adapter stack over one private migrated database,
// wired exactly like cmd/api's buildAdapters + buildServer for the plan
// flows: real Postgres repos, the real UnitOfWork, the production fanout
// encoder (ADR 0005: every event on the integration AND the analytics
// topic). No in-memory repo fakes.
type wiredStack struct {
	register *usecases.RegisterProcessCapacityConstraint
	paths    *usecases.RegisterProcessPath
	create   *usecases.CreateCapacityPlan
	publish  *usecases.PublishCapacityPlan
	read     *usecases.GetProcessPathCapacity
	plans    *postgres.CapacityPlanRepo
	pool     *pgxpool.Pool
}

func newWiredStack(t *testing.T) *wiredStack {
	t.Helper()
	ctx := context.Background()
	pool, err := postgres.NewPool(ctx, wireMigratedDB(t))
	if err != nil {
		t.Fatalf("open pool: %v", err)
	}
	t.Cleanup(pool.Close)

	pcs := postgres.NewProcessCapacityRepo(pool)
	pathRepo := postgres.NewProcessPathRepo(pool)
	plans := postgres.NewCapacityPlanRepo(pool)
	ob := postgres.NewOutboxRepo(pool)
	standards := postgres.NewStationStandardRepo(pool)
	tally := postgres.NewStorageTallyRepo(pool)
	uow := postgres.NewUnitOfWork(pool)
	encoder := outboundkafka.NewFanoutEncoder()

	pathCapacity := &usecases.GetProcessPathCapacity{
		ProcessPaths: pathRepo, ProcessCapacities: pcs, StationStandards: standards, Tally: tally,
	}
	return &wiredStack{
		register: &usecases.RegisterProcessCapacityConstraint{Repo: pcs},
		paths:    &usecases.RegisterProcessPath{Repo: pathRepo},
		create: &usecases.CreateCapacityPlan{
			PathCapacity: pathCapacity, Plans: plans, Outbox: ob,
			Encoder: encoder, UnitOfWork: uow,
			Demand: &usecases.GetExpectedDemand{Demand: postgres.NewOrderDemandRepo(pool)},
		},
		publish: &usecases.PublishCapacityPlan{Plans: plans, Outbox: ob, Encoder: encoder, UnitOfWork: uow},
		read:    pathCapacity,
		plans:   plans,
		pool:    pool,
	}
}

// wireSeed registers the section-43 worked-example capacities (PICK 4000
// UNIT/h, REBIN 2500 UNIT/h, PACK 1800 PACKAGE/h — path capacity 1000
// ORDER/h, bottleneck REBIN) and the pick-rebin-pack path, through the real
// write use cases.
func (s *wiredStack) wireSeed(t *testing.T) {
	t.Helper()
	ctx := context.Background()
	for _, reg := range []struct {
		process processcapacity.ProcessType
		qty     float64
		unit    processcapacity.CapacityUnit
	}{{"PICK", 4000, processcapacity.UnitUnit}, {"REBIN", 2500, processcapacity.UnitUnit}, {"PACK", 1800, processcapacity.UnitPackage}} {
		if _, err := s.register.Handle(ctx, usecases.RegisterProcessCapacityConstraintCommand{
			ProcessType: reg.process, Location: "IT-WIRE-LOC",
			WindowStart:    time.Date(2026, 10, 5, 8, 0, 0, 0, time.UTC),
			WindowEnd:      time.Date(2026, 10, 5, 16, 0, 0, 0, time.UTC),
			ConstraintType: processcapacity.ConstraintLabor, Quantity: reg.qty, Unit: reg.unit, Period: time.Hour,
		}); err != nil {
			t.Fatalf("register %s: %v", reg.process, err)
		}
	}
	if _, err := s.paths.Handle(ctx, usecases.RegisterProcessPathCommand{
		ID: "pick-rebin-pack", Name: "Pick-Rebin-Pack", Steps: []processpath.ProcessType{"PICK", "REBIN", "PACK"},
	}); err != nil {
		t.Fatalf("register path: %v", err)
	}
}

// The aggregate lifecycle end to end over real Postgres: create a DRAFT
// shortage plan, publish it, and read the persisted state and the queued
// events back. Create queues exactly CapacityPlanCreated; publish queues
// CapacityPlanPublished + CapacityShortageDetected + BottleneckDetected —
// each on BOTH streams (ADR 0005).
func TestUsecases_PlanLifecycleOverRealPostgres(t *testing.T) {
	s := newWiredStack(t)
	s.wireSeed(t)
	ctx := context.Background()
	start := time.Date(2026, 10, 5, 8, 0, 0, 0, time.UTC)
	end := time.Date(2026, 10, 5, 16, 0, 0, 0, time.UTC)

	upo, ppo := 2.5, 1.0
	plan, err := s.create.Handle(ctx, usecases.CreateCapacityPlanCommand{
		WarehouseID: "WH-1", SiteID: "SIM1", Location: "IT-WIRE-LOC",
		WindowStart: start, WindowEnd: end, ProcessPathID: "pick-rebin-pack",
		AssignedDemand: 12000, UnitsPerOrder: &upo, PackagesPerOrder: &ppo,
	})
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	if plan.Status() != capacityplan.StatusDraft ||
		plan.PathCapacity() != 1000 || plan.CapacityOverWindow() != 8000 ||
		plan.Shortage() != 4000 || plan.BottleneckStep() != "REBIN" {
		t.Fatalf("created plan = %+v", plan)
	}

	// The read model agrees: a reload through the real repo returns the
	// persisted DRAFT state unchanged.
	reloaded, err := s.plans.FindByID(ctx, plan.ID())
	if err != nil || reloaded == nil {
		t.Fatalf("FindByID = %v, %v", reloaded, err)
	}
	if reloaded.Status() != capacityplan.StatusDraft || reloaded.Shortage() != 4000 || !reloaded.PublishedAt().IsZero() {
		t.Fatalf("reloaded = status %s shortage %v publishedAt %v", reloaded.Status(), reloaded.Shortage(), reloaded.PublishedAt())
	}

	// State change: publish, then the persisted plan is PUBLISHED.
	published, err := s.publish.Handle(ctx, plan.ID())
	if err != nil {
		t.Fatalf("publish: %v", err)
	}
	if published.Status() != capacityplan.StatusPublished || published.PublishedAt().IsZero() {
		t.Fatalf("published = %+v", published)
	}
	reloaded, _ = s.plans.FindByID(ctx, plan.ID())
	if reloaded.Status() != capacityplan.StatusPublished || reloaded.PublishedAt().IsZero() {
		t.Fatalf("reloaded after publish = %s at %v", reloaded.Status(), reloaded.PublishedAt())
	}

	// Published events: the outbox holds the four occurrences, twice each
	// (integration + analytics), in row order.
	rows, err := s.pool.Query(ctx, `SELECT event_type FROM outbox_events ORDER BY id`)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	var types []string
	for rows.Next() {
		var typ string
		if err := rows.Scan(&typ); err != nil {
			t.Fatal(err)
		}
		types = append(types, typ)
	}
	prefix := "com.warehouse.wes.warehouse-planning.capacityplan."
	want := []string{
		prefix + "CapacityPlanCreated", prefix + "CapacityPlanCreated",
		prefix + "CapacityPlanPublished", prefix + "CapacityPlanPublished",
		prefix + "CapacityShortageDetected", prefix + "CapacityShortageDetected",
		prefix + "BottleneckDetected", prefix + "BottleneckDetected",
	}
	if len(types) != len(want) {
		t.Fatalf("outbox = %v, want %v", types, want)
	}
	for i := range want {
		if types[i] != want[i] {
			t.Fatalf("outbox[%d] = %s, want %s (all: %v)", i, types[i], want[i], types)
		}
	}

	// A second publish is the domain's double-publish rejection.
	if _, err := s.publish.Handle(ctx, plan.ID()); err != capacityplan.ErrAlreadyPublished {
		t.Fatalf("second publish err = %v, want ErrAlreadyPublished", err)
	}
}

// The shared read composition over the real repos: the path capacity use
// case reads back exactly what the write use cases registered (worked
// example: 1000 ORDER/h end to end, REBIN binding).
func TestUsecases_PathCapacityReadsWhatRegistrationWrote(t *testing.T) {
	s := newWiredStack(t)
	s.wireSeed(t)
	ctx := context.Background()

	result, err := s.read.Handle(ctx, usecases.GetProcessPathCapacityCommand{
		ProcessPathID: "pick-rebin-pack", Location: "IT-WIRE-LOC",
		WindowStart:      time.Date(2026, 10, 5, 8, 0, 0, 0, time.UTC),
		WindowEnd:        time.Date(2026, 10, 5, 16, 0, 0, 0, time.UTC),
		UnitsPerOrder:    func() *float64 { v := 2.5; return &v }(),
		PackagesPerOrder: func() *float64 { v := 1.0; return &v }(),
	})
	if err != nil {
		t.Fatalf("path capacity: %v", err)
	}
	if result.NormalizedRate.Quantity() != 1000 || result.NormalizedRate.Unit() != processcapacity.UnitOrder ||
		result.BottleneckStep != "REBIN" {
		t.Fatalf("path capacity = %v %v bottleneck %v", result.NormalizedRate.Quantity(), result.NormalizedRate.Unit(), result.BottleneckStep)
	}
}

// A unit-mismatched registration against the SAME process/location/window
// is the domain's conflict, surfaced through the real repo round trip.
func TestUsecases_RegistrationConflictSurfacesThroughRealPostgres(t *testing.T) {
	s := newWiredStack(t)
	ctx := context.Background()
	cmd := usecases.RegisterProcessCapacityConstraintCommand{
		ProcessType: "PACK", Location: "IT-WIRE-LOC",
		WindowStart:    time.Date(2026, 10, 5, 8, 0, 0, 0, time.UTC),
		WindowEnd:      time.Date(2026, 10, 5, 16, 0, 0, 0, time.UTC),
		ConstraintType: processcapacity.ConstraintLabor, Quantity: 100, Unit: processcapacity.UnitPackage, Period: time.Hour,
	}
	if _, err := s.register.Handle(ctx, cmd); err != nil {
		t.Fatalf("first registration: %v", err)
	}
	cmd.Unit = processcapacity.UnitUnit // conflicts with the stored PACKAGE
	if _, err := s.register.Handle(ctx, cmd); err != processcapacity.ErrUnitMismatch {
		t.Fatalf("mismatched unit err = %v, want ErrUnitMismatch", err)
	}
}
