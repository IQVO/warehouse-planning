// Command mcp is the composition root for the warehouse-planning MCP server:
// it wires env config to outbound adapters, adapters to the SAME use cases
// the REST adapter uses, and those to the inbound MCP adapter, then serves
// MCP over Streamable HTTP (and nothing else: no stdio, no SSE). It is a
// second, independent deployable alongside cmd/api, per ADR-0008.
//
// There is NO auth of any kind (fleet-wide revert 2026-09-11): no keys, no
// bearer checks. Access control is the in-cluster ClusterIP boundary.
//
// What this binary deliberately does NOT do: it never starts the outbox
// relay and never dials Kafka. create_capacity_plan/publish_capacity_plan
// insert their CloudEvents into the transactional outbox inside the
// UnitOfWork (outboundkafka.NewEncoder only builds envelopes in memory);
// the relay that drains the outbox runs in cmd/api only.
package main

import (
	"context"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	inboundmcp "github.com/claudioed/warehouse-planning/internal/adapters/inbound/mcp"
	outboundkafka "github.com/claudioed/warehouse-planning/internal/adapters/outbound/kafka"
	"github.com/claudioed/warehouse-planning/internal/adapters/outbound/memory"
	"github.com/claudioed/warehouse-planning/internal/adapters/outbound/postgres"
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

	httpAddr := getenv("MCP_ADDR", ":8090")
	databaseURL := os.Getenv("DATABASE_URL")

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

// buildDeps selects in-memory vs Postgres adapters the way cmd/api does: no
// DATABASE_URL means in-memory repos (runnable locally); a URL means migrate
// then connect a pgx pool. The returned close func releases the pool.
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

	var (
		pcRepo     ports.ProcessCapacityRepository
		standards  ports.StationStandardRepository
		tallyRead  ports.StorageTallyReader
		pathRepo   ports.ProcessPathRepository
		planRepo   ports.CapacityPlanRepository
		demandRepo ports.OrderDemandRepository
		outboxRep  ports.OutboxRepository
		uow        ports.UnitOfWork
		closeFn    = noop
	)

	if databaseURL == "" {
		logger.Info("DATABASE_URL not configured; using in-memory adapters")
		pc, plans, outboxRepo := memory.NewProcessCapacityRepo(), memory.NewCapacityPlanRepo(), memory.NewOutboxRepo()
		pcRepo, pathRepo, planRepo, outboxRep = pc, memory.NewProcessPathRepo(), plans, outboxRepo
		standards, tallyRead = memory.NewStationStandardRepo(), memory.NewStorageTallyRepo()
		demandRepo = memory.NewOrderDemandRepo()
		// Participants make the in-memory UoW roll back on error too.
		uow = memory.NewUnitOfWork(pc, plans, outboxRepo)
	} else {
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
		pcRepo, pathRepo = postgres.NewProcessCapacityRepo(pool), postgres.NewProcessPathRepo(pool)
		standards, tallyRead = postgres.NewStationStandardRepo(pool), postgres.NewStorageTallyRepo(pool)
		planRepo, outboxRep = postgres.NewCapacityPlanRepo(pool), postgres.NewOutboxRepo(pool)
		demandRepo = postgres.NewOrderDemandRepo(pool)
		uow = postgres.NewUnitOfWork(pool)
		closeFn = pool.Close
	}

	pathCapacity := &usecases.GetProcessPathCapacity{
		ProcessPaths: pathRepo, ProcessCapacities: pcRepo, StationStandards: standards, Tally: tallyRead,
	}
	expectedDemand := &usecases.GetExpectedDemand{Demand: demandRepo}
	encoder := outboundkafka.NewEncoder()
	return inboundmcp.Deps{
		RegisterProcessCapacityConstraint: &usecases.RegisterProcessCapacityConstraint{Repo: pcRepo},
		ProcessCapacities:                 pcRepo,
		RegisterProcessPath:               &usecases.RegisterProcessPath{Repo: pathRepo},
		GetProcessPathCapacity:            pathCapacity,
		CreateCapacityPlan: &usecases.CreateCapacityPlan{
			PathCapacity: pathCapacity, Plans: planRepo, Outbox: outboxRep, Encoder: encoder, UnitOfWork: uow,
			Demand: expectedDemand,
		},
		PublishCapacityPlan: &usecases.PublishCapacityPlan{
			Plans: planRepo, Outbox: outboxRep, Encoder: encoder, UnitOfWork: uow,
		},
		CapacityPlans: planRepo,

		DeclareStationStandard: &usecases.DeclareStationStandard{Repo: standards},
		StationStandards:       standards,
		GetStorageCapacity:     &usecases.GetStorageCapacity{Tally: tallyRead},
		GetExpectedDemand:      expectedDemand,
	}, closeFn, nil
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
