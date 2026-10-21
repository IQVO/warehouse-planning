// Command mcp is the composition root for the warehouse-planning MCP server:
// it wires env config to outbound adapters, adapters to the SAME use cases
// the REST adapter uses, and those to the inbound MCP adapter, then serves
// MCP over Streamable HTTP (and nothing else: no stdio, no SSE). It is a
// second, independent deployable alongside cmd/api, per ADR-0008.
//
// There is NO auth of any kind (fleet-wide revert 2026-09-11): no keys, no
// bearer checks. Access control is the in-cluster ClusterIP boundary.
//
// It runs on real Postgres only (ADR 0016): DATABASE_URL is mandatory and a
// missing one is a boot error that names it; there is no in-memory mode.
//
// What this binary deliberately does NOT do: it never starts the outbox
// relay and never dials Kafka. create_capacity_plan/publish_capacity_plan
// insert their CloudEvents into the transactional outbox inside the
// UnitOfWork (outboundkafka.NewFanoutEncoder only builds envelopes in memory,
// one integration and one analytics message per event);
// the relay that drains the outbox runs in cmd/api only.
package main

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	inboundmcp "github.com/claudioed/warehouse-planning/internal/adapters/inbound/mcp"
	outboundkafka "github.com/claudioed/warehouse-planning/internal/adapters/outbound/kafka"
	"github.com/claudioed/warehouse-planning/internal/adapters/outbound/postgres"
	"github.com/claudioed/warehouse-planning/internal/adapters/outbound/telemetry"
	"github.com/claudioed/warehouse-planning/internal/application/ports"
	"github.com/claudioed/warehouse-planning/internal/application/usecases"
	"github.com/claudioed/warehouse-planning/internal/bootretry"
)

// shutdownTimeout bounds the HTTP server's graceful drain.
const shutdownTimeout = 10 * time.Second

// defaultMigrationsPath is this service's migrations/ directory relative to
// the repo root, the same default cmd/api uses.
const defaultMigrationsPath = "internal/adapters/outbound/postgres/migrations"

func main() {
	if err := run(); err != nil {
		slog.Error("mcp server exited with error", "error", err)
		os.Exit(1)
	}
}

func run() error {
	logger := newLogger(getenv("LOG_LEVEL", "info"))
	slog.SetDefault(logger)

	// Misconfiguration is refused before anything is opened or bound.
	databaseURL := os.Getenv("DATABASE_URL")
	if databaseURL == "" {
		return errMissingDatabaseURL
	}

	// Standard metrics (docs/adr/0011), same adapter cmd/api uses.
	otelCtx, otelCancel := context.WithTimeout(context.Background(), 10*time.Second)
	otelShutdown, err := telemetry.Setup(otelCtx, mcpServiceName, serviceVersion(), getenv("OTEL_EXPORTER_OTLP_ENDPOINT", telemetry.DefaultOTLPEndpoint))
	otelCancel()
	if err != nil {
		return err
	}
	defer func() {
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := otelShutdown(shutdownCtx); err != nil {
			logger.Error("telemetry shutdown failed", "error", err)
		}
	}()

	httpAddr := getenv("MCP_ADDR", ":8090")

	deps, closeAdapters, err := buildDeps(context.Background(), logger, databaseURL,
		getenv("MIGRATIONS_DATABASE_URL", databaseURL),
		getenv("MIGRATIONS_PATH", defaultMigrationsPath))
	if err != nil {
		return err
	}
	defer closeAdapters()

	srv := &http.Server{
		Addr:              httpAddr,
		Handler:           newRouter(inboundmcp.Handler(inboundmcp.NewServer(deps))),
		ReadHeaderTimeout: 5 * time.Second,
	}
	return serveMCP(logger, srv)
}

// errMissingDatabaseURL is the boot error of an unset DATABASE_URL: there is
// no in-memory mode (ADR 0016).
var errMissingDatabaseURL = errors.New("DATABASE_URL is required: this context runs on real Postgres, there is no in-memory mode")

// buildDeps migrates then connects a pgx pool and wires the Postgres
// repositories into the MCP dependencies. The returned close func releases
// the pool. An empty databaseURL is a boot error.
//
// Like every sibling's cmd/mcp (and unlike a pure read replica) this binary
// runs the idempotent golang-migrate step on start, so it can boot against
// a fresh database before cmd/api has; the migrator's advisory lock makes
// concurrent starts safe. migrationsDatabaseURL is used ONLY for that step
// (a DIRECT Postgres DSN in clusters where DATABASE_URL points at PgBouncer,
// which cannot honour the session-scoped advisory lock); the runtime pool
// always uses databaseURL.
func buildDeps(ctx context.Context, logger *slog.Logger, databaseURL, migrationsDatabaseURL, migrationsPath string) (inboundmcp.Deps, func(), error) {
	noop := func() {}
	if databaseURL == "" {
		return inboundmcp.Deps{}, noop, errMissingDatabaseURL
	}

	// The first outbound dial of an injected pod is reset ~10s after
	// start (Istio native sidecars), so migrations and the first ping
	// retry with backoff; on exhaustion the process still refuses to boot.
	if err := bootretry.Retry(ctx, logger, "run migrations", func() error {
		return postgres.RunMigrations(migrationsDatabaseURL, migrationsPath)
	}); err != nil {
		return inboundmcp.Deps{}, noop, err
	}
	pool, err := postgres.NewPool(ctx, databaseURL)
	if err != nil {
		return inboundmcp.Deps{}, noop, err
	}
	if err := bootretry.Retry(ctx, logger, "ping postgres", func() error { return pool.Ping(ctx) }); err != nil {
		pool.Close()
		return inboundmcp.Deps{}, noop, err
	}
	logger.Info("postgres adapters configured", "migrations_path", migrationsPath)
	deps, err := newDeps(repositories{
		processCapacities: postgres.NewProcessCapacityRepo(pool),
		processPaths:      postgres.NewProcessPathRepo(pool),
		standards:         postgres.NewStationStandardRepo(pool),
		tally:             postgres.NewStorageTallyRepo(pool),
		plans:             postgres.NewCapacityPlanRepo(pool),
		outbox:            postgres.NewOutboxRepo(pool),
		demand:            postgres.NewOrderDemandRepo(pool),
		uow:               postgres.NewUnitOfWork(pool),

		inboundAppointments: postgres.NewInboundAppointmentRepo(pool),
		inboundStandards:    postgres.NewInboundStandardRepo(pool),
		observedCapacity:    postgres.NewObservedCapacityRepo(pool),
	})
	if err != nil {
		pool.Close()
		return inboundmcp.Deps{}, noop, err
	}
	return deps, pool.Close, nil
}

// repositories is the set of outbound ports the MCP use cases need. buildDeps
// fills it with the Postgres adapters; tests fill it with the in-memory test
// doubles.
type repositories struct {
	processCapacities ports.ProcessCapacityRepository
	processPaths      ports.ProcessPathRepository
	standards         ports.StationStandardRepository
	tally             ports.StorageTallyReader
	plans             ports.CapacityPlanRepository
	outbox            ports.OutboxRepository
	demand            ports.OrderDemandRepository
	uow               ports.UnitOfWork

	// ADR 0017.
	inboundAppointments ports.InboundAppointmentRepository
	inboundStandards    ports.InboundStandardRepository
	observedCapacity    ports.ObservedCapacityRepository
}

// newDeps assembles the MCP dependencies (the SAME use cases the REST adapter
// uses) over r.
func newDeps(r repositories) (inboundmcp.Deps, error) {
	pathCapacity := &usecases.GetProcessPathCapacity{
		ProcessPaths: r.processPaths, ProcessCapacities: r.processCapacities, StationStandards: r.standards, Tally: r.tally,
	}
	expectedDemand := &usecases.GetExpectedDemand{Demand: r.demand}
	inboundDemand := &usecases.GetInboundDemand{Appointments: r.inboundAppointments, Standards: r.inboundStandards}
	encoder := outboundkafka.NewFanoutEncoder()
	planMetrics, err := telemetry.NewPlanMetrics()
	if err != nil {
		return inboundmcp.Deps{}, err
	}
	return inboundmcp.Deps{
		RegisterProcessCapacityConstraint: &usecases.RegisterProcessCapacityConstraint{
			Repo: r.processCapacities,
			// Same contract as cmd/api: the aggregate's events are
			// outboxed atomically with the save (integration stream only;
			// the analytics stream is plan facts only).
			Outbox: r.outbox, Encoder: encoder, UnitOfWork: r.uow,
		},
		ProcessCapacities:      r.processCapacities,
		RegisterProcessPath:    &usecases.RegisterProcessPath{Repo: r.processPaths},
		GetProcessPathCapacity: pathCapacity,
		CreateCapacityPlan: &usecases.CreateCapacityPlan{
			PathCapacity: pathCapacity, Plans: r.plans, Outbox: r.outbox, Encoder: encoder, UnitOfWork: r.uow,
			Demand: expectedDemand, Inbound: inboundDemand, Metrics: planMetrics,
		},
		PublishCapacityPlan: &usecases.PublishCapacityPlan{
			Plans: r.plans, Outbox: r.outbox, Encoder: encoder, UnitOfWork: r.uow,
		},
		CapacityPlans: r.plans,

		DeclareStationStandard: &usecases.DeclareStationStandard{Repo: r.standards},
		StationStandards:       r.standards,
		GetStorageCapacity:     &usecases.GetStorageCapacity{Tally: r.tally},
		GetExpectedDemand:      expectedDemand,

		DeclareInboundLabourStandard: &usecases.DeclareInboundLabourStandard{Repo: r.inboundStandards},
		GetInboundDemand:             inboundDemand,
		GetCapacityPlanVariance:      &usecases.GetCapacityPlanVariance{Plans: r.plans, Observed: r.observedCapacity},
	}, nil
}

// serveMCP runs srv until it fails or SIGINT/SIGTERM arrives, then drains it
// gracefully.
func serveMCP(logger *slog.Logger, srv *http.Server) error {
	serverErr := make(chan error, 1)
	go func() {
		logger.Info("mcp server listening (Streamable HTTP)", "addr", srv.Addr)
		serverErr <- srv.ListenAndServe()
	}()

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	select {
	case err := <-serverErr:
		if err != nil && err != http.ErrServerClosed {
			return err
		}
		return nil
	case <-ctx.Done():
	}

	shutdownCtx, cancel := context.WithTimeout(context.Background(), shutdownTimeout)
	defer cancel()
	return srv.Shutdown(shutdownCtx)
}

// newLogger builds the process-wide structured logger. LOG_LEVEL maps
// debug|info|warn|error (case-insensitive), defaulting to Info.
func newLogger(level string) *slog.Logger {
	var lvl slog.Level
	switch strings.ToLower(level) {
	case "debug":
		lvl = slog.LevelDebug
	case "warn", "warning":
		lvl = slog.LevelWarn
	case "error":
		lvl = slog.LevelError
	default:
		lvl = slog.LevelInfo
	}
	return slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: lvl}))
}

func getenv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

// serviceVersion is this binary's OTel service.version resource
// attribute. SERVICE_VERSION is set by the chart from the image tag; a
// local run without it reports "dev".
func serviceVersion() string {
	return getenv("SERVICE_VERSION", "dev")
}
