// Command a2a is the composition root for the warehouse-planning A2A agent:
// it wires env config to outbound adapters, adapters to the SAME use cases
// the REST and MCP adapters use, and those to the inbound A2A adapter, then
// serves Agent2Agent JSON-RPC over HTTP with the Agent Card at the well-known
// path. It is a third, independent deployable alongside cmd/api and cmd/mcp,
// per ADR 0018.
//
// There is NO auth of any kind (fleet-wide revert 2026-09-11): no keys, no
// bearer checks. Access control is the in-cluster ClusterIP boundary.
//
// It runs on real Postgres only (ADR 0016): DATABASE_URL is mandatory and a
// missing one is a boot error that names it; there is no in-memory mode.
// A2A_AGENT_URL (the absolute http(s) URL the Agent Card advertises) is
// mandatory too. Both are checked before telemetry, any connection and the
// listener.
//
// What this binary deliberately does NOT do: it never starts the outbox
// relay and never dials Kafka. evaluate_capacity_plan inserts the plan's
// CloudEvents into the transactional outbox inside the UnitOfWork
// (outboundkafka.NewFanoutEncoder only builds envelopes in memory, one
// integration and one analytics message per event); the relay that drains the
// outbox runs in cmd/api only.
//
// Environment:
//
//	DATABASE_URL             REQUIRED. Postgres DSN
//	A2A_AGENT_URL            REQUIRED. Absolute http(s) URL of this agent's
//	                         JSON-RPC endpoint, advertised on the Agent Card
//	MIGRATIONS_DATABASE_URL  direct DSN for the boot migrations (default DATABASE_URL)
//	MIGRATIONS_PATH          migrations directory (default internal/adapters/outbound/postgres/migrations)
//	A2A_ADDR                 listen address (default :8093)
//	LOG_LEVEL                debug|info|warn|error (default info)
package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	inbounda2a "github.com/claudioed/warehouse-planning/internal/adapters/inbound/a2a"
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
		slog.Error("a2a agent exited with error", "error", err)
		os.Exit(1)
	}
}

func run() error {
	logger := newLogger(getenv("LOG_LEVEL", "info"))
	slog.SetDefault(logger)

	// Misconfiguration is refused before anything is opened or bound.
	cfg, err := loadBootConfig()
	if err != nil {
		return err
	}

	// Standard metrics (docs/adr/0011), same adapter cmd/api uses.
	otelCtx, otelCancel := context.WithTimeout(context.Background(), 10*time.Second)
	otelShutdown, err := telemetry.Setup(otelCtx, a2aServiceName, serviceVersion(), getenv("OTEL_EXPORTER_OTLP_ENDPOINT", telemetry.DefaultOTLPEndpoint))
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

	httpAddr := getenv("A2A_ADDR", ":8093")

	deps, closeAdapters, err := buildDeps(context.Background(), logger, cfg.databaseURL,
		getenv("MIGRATIONS_DATABASE_URL", cfg.databaseURL),
		getenv("MIGRATIONS_PATH", defaultMigrationsPath))
	if err != nil {
		return err
	}
	defer closeAdapters()

	srv := &http.Server{
		Addr: httpAddr,
		Handler: newRouter(inbounda2a.Handler(inbounda2a.NewExecutor(deps)),
			inbounda2a.Card(inbounda2a.Config{AgentURL: cfg.agentURL, Version: serviceVersion()})),
		ReadHeaderTimeout: 5 * time.Second,
	}
	return serveA2A(logger, srv)
}

// errMissingDatabaseURL is the boot error of an unset DATABASE_URL: there is
// no in-memory mode (ADR 0016).
var errMissingDatabaseURL = errors.New("DATABASE_URL is required: this context runs on real Postgres, there is no in-memory mode")

// errNoAgentURL is the boot error for a missing A2A_AGENT_URL: the Agent Card
// must advertise the absolute URL clients dial (ADR 0018).
var errNoAgentURL = errors.New("A2A_AGENT_URL is required: the Agent Card must advertise the absolute URL of this agent's JSON-RPC endpoint (ADR 0018), e.g. http://<release>-a2a.<ns>.svc.cluster.local:8093/")

// bootConfig is the mandatory configuration of cmd/a2a, with no default and no
// degraded mode behind any of it.
type bootConfig struct {
	databaseURL string
	agentURL    string
}

// checkAgentURL validates A2A_AGENT_URL: an absolute http(s) URL. The card is
// the discovery contract -- a relative or schemeless URL is a misconfiguration
// every client would hit, so it is refused at boot, never silently served.
func checkAgentURL(raw string) error {
	if raw == "" {
		return errNoAgentURL
	}
	u, err := url.Parse(raw)
	if err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" {
		return fmt.Errorf("A2A_AGENT_URL=%q is not an absolute http(s) URL: the Agent Card must advertise the endpoint clients dial (ADR 0018)", raw)
	}
	return nil
}

// loadBootConfig reads and validates the mandatory environment. Every problem
// is reported at once, each naming its variable, so the process fails fast at
// boot, before telemetry, any connection and the listener.
func loadBootConfig() (bootConfig, error) {
	cfg := bootConfig{
		databaseURL: os.Getenv("DATABASE_URL"),
		agentURL:    strings.TrimSpace(os.Getenv("A2A_AGENT_URL")),
	}
	var errs []error
	if cfg.databaseURL == "" {
		errs = append(errs, errMissingDatabaseURL)
	}
	if err := checkAgentURL(cfg.agentURL); err != nil {
		errs = append(errs, err)
	}
	if err := errors.Join(errs...); err != nil {
		return bootConfig{}, err
	}
	return cfg, nil
}

// buildDeps migrates then connects a pgx pool and wires the Postgres
// repositories into the A2A dependencies. The returned close func releases
// the pool. An empty databaseURL is a boot error.
//
// Like cmd/mcp (and unlike a pure read replica) this binary
// runs the idempotent golang-migrate step on start, so it can boot against
// a fresh database before cmd/api has; the migrator's advisory lock makes
// concurrent starts safe. migrationsDatabaseURL is used ONLY for that step
// (a DIRECT Postgres DSN in clusters where DATABASE_URL points at PgBouncer,
// which cannot honour the session-scoped advisory lock); the runtime pool
// always uses databaseURL.
func buildDeps(ctx context.Context, logger *slog.Logger, databaseURL, migrationsDatabaseURL, migrationsPath string) (inbounda2a.Deps, func(), error) {
	noop := func() {}
	if databaseURL == "" {
		return inbounda2a.Deps{}, noop, errMissingDatabaseURL
	}

	// The first outbound dial of an injected pod is reset ~10s after
	// start (Istio native sidecars), so migrations and the first ping
	// retry with backoff; on exhaustion the process still refuses to boot.
	if err := bootretry.Retry(ctx, logger, "run migrations", func() error {
		return postgres.RunMigrations(migrationsDatabaseURL, migrationsPath)
	}); err != nil {
		return inbounda2a.Deps{}, noop, err
	}
	pool, err := postgres.NewPool(ctx, databaseURL)
	if err != nil {
		return inbounda2a.Deps{}, noop, err
	}
	if err := bootretry.Retry(ctx, logger, "ping postgres", func() error { return pool.Ping(ctx) }); err != nil {
		pool.Close()
		return inbounda2a.Deps{}, noop, err
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
	})
	if err != nil {
		pool.Close()
		return inbounda2a.Deps{}, noop, err
	}
	return deps, pool.Close, nil
}

// repositories is the set of outbound ports the A2A use cases need. buildDeps
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
}

// newDeps assembles the A2A dependencies (the SAME use cases the REST and MCP
// adapters use) over r.
func newDeps(r repositories) (inbounda2a.Deps, error) {
	pathCapacity := &usecases.GetProcessPathCapacity{
		ProcessPaths: r.processPaths, ProcessCapacities: r.processCapacities, StationStandards: r.standards, Tally: r.tally,
	}
	expectedDemand := &usecases.GetExpectedDemand{Demand: r.demand}
	inboundDemand := &usecases.GetInboundDemand{Appointments: r.inboundAppointments, Standards: r.inboundStandards}
	encoder := outboundkafka.NewFanoutEncoder()
	planMetrics, err := telemetry.NewPlanMetrics()
	if err != nil {
		return inbounda2a.Deps{}, err
	}
	return inbounda2a.Deps{
		GetProcessPathCapacity: pathCapacity,
		CreateCapacityPlan: &usecases.CreateCapacityPlan{
			PathCapacity: pathCapacity, Plans: r.plans, Outbox: r.outbox, Encoder: encoder, UnitOfWork: r.uow,
			Demand: expectedDemand, Inbound: inboundDemand, Metrics: planMetrics,
		},
	}, nil
}

// serveA2A runs srv until it fails or SIGINT/SIGTERM arrives, then drains it
// gracefully.
func serveA2A(logger *slog.Logger, srv *http.Server) error {
	serverErr := make(chan error, 1)
	go func() {
		logger.Info("a2a agent listening (JSON-RPC over HTTP)", "addr", srv.Addr)
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
