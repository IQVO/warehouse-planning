// Command api is the warehouse-planning composition root: it wires env
// config into adapters, adapters into use cases, and use cases into the HTTP
// router, the five Kafka consumers (labor capacity, storage capacity, order
// demand, inbound dock appointments, observed task completions) and the outbox relay, all on real Postgres and real Kafka (ADR
// 0016): there is no in-memory mode, no log sink and no consumer that is off.
//
// Environment:
//
//	HTTP_ADDR                        listen address (default :8080)
//	DATABASE_URL                     Postgres DSN (REQUIRED: there is no in-memory mode, ADR 0016)
//	MIGRATIONS_DATABASE_URL          direct DSN for the boot migrations (default DATABASE_URL)
//	MIGRATIONS_PATH                  migrations directory (default internal/adapters/outbound/postgres/migrations)
//	KAFKA_BROKERS                    comma-separated brokers (REQUIRED: relay, consumers, /readyz)
//	EVENT_PUBLISHER                  kafka (default kafka); log is a boot error
//	OUTBOX_RELAY_INTERVAL            relay poll interval (Go duration or seconds, default 1s)
//	OUTBOX_MAX_ATTEMPTS              failures before the relay excludes a row (default 20, 0 = unlimited; ADR 0013)
//	LABOR_CAPACITY_CONSUMER_GROUP    stable group id of the labor-capacity consumer (REQUIRED)
//	STORAGE_CAPACITY_CONSUMER_GROUP  stable group id of the storage-capacity consumer (REQUIRED)
//	DEMAND_CONSUMER_GROUP            stable group id of the order-demand consumer (REQUIRED)
//	INBOUND_DEMAND_CONSUMER_GROUP    stable group id of the dock-appointment consumer (REQUIRED, ADR 0017)
//	OBSERVED_CAPACITY_CONSUMER_GROUP stable group id of the task-completion consumer (REQUIRED, ADR 0017)
//	DEMAND_SITE_ID                   the one site every consumed order, dock appointment and task completion is attributed to (REQUIRED)
//	OTEL_EXPORTER_OTLP_ENDPOINT      OTLP/gRPC collector (default localhost:4317)
//	SHUTDOWN_DRAIN_DELAY             wait after flipping /readyz before closing (default 5s)
//	LOG_LEVEL, SERVICE_VERSION, ENVIRONMENT, CORS_ALLOWED_ORIGINS
//
// A missing or rejected variable is a boot error that names it, raised before
// the listener binds. GET /readyz is 200 only while Postgres answers a ping
// AND a Kafka broker answers a metadata request (verdict cached 3s, bounded
// 2s); /healthz is liveness only. The process exits non-zero when the relay or
// a consumer returns on its own, so the orchestrator restarts it.
package main

import (
	"context"
	"log/slog"
	"net/http"
	"os"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	inboundhttp "github.com/claudioed/warehouse-planning/internal/adapters/inbound/http"
	outboundkafka "github.com/claudioed/warehouse-planning/internal/adapters/outbound/kafka"
	outboxrelay "github.com/claudioed/warehouse-planning/internal/adapters/outbound/outbox"
	"github.com/claudioed/warehouse-planning/internal/adapters/outbound/postgres"
	"github.com/claudioed/warehouse-planning/internal/application/ports"
	"github.com/claudioed/warehouse-planning/internal/application/usecases"
	"github.com/claudioed/warehouse-planning/internal/bootretry"
)

// shutdownTimeout bounds the HTTP server's graceful drain.
const shutdownTimeout = 10 * time.Second

// consumerDrainTimeout bounds how long shutdown waits for each Kafka
// consumer's Run loop to actually return after its context is cancelled,
// mirroring the HTTP server's own Shutdown budget -- a consumer that does
// not stop within this window is logged and shutdown proceeds anyway.
const consumerDrainTimeout = 10 * time.Second

// defaultMigrationsPath is this service's migrations/ directory relative
// to the repo root (the WORKDIR a container image or `go run ./cmd/api`
// from the repo root both use).
const defaultMigrationsPath = "internal/adapters/outbound/postgres/migrations"

func main() {
	if err := run(context.Background()); err != nil {
		slog.Error("service exited with error", "error", err)
		os.Exit(1)
	}
}

// run boots the service and serves until SIGINT/SIGTERM or until ctx is
// cancelled (the integration test stops the real entry point that way).
func run(ctx context.Context) error {
	logger := newLogger(getenv("LOG_LEVEL", "info"))
	slog.SetDefault(logger)

	// Misconfiguration is refused before anything is opened or bound (ADR 0016).
	rt, err := loadRuntime(os.Getenv)
	if err != nil {
		return err
	}

	otelShutdown, planMetrics, err := setupTelemetry(logger)
	if err != nil {
		return err
	}
	defer otelShutdown()

	httpAddr := getenv("HTTP_ADDR", ":8080")
	migrationsPath := getenv("MIGRATIONS_PATH", defaultMigrationsPath)

	// closeAdapters (pool.Close) is deferred FIRST so it runs LAST, after the
	// HTTP drain, the consumers and the relay have all stopped.
	ad, closeAdapters, err := buildAdapters(ctx, os.Getenv(envDatabaseURL), migrationsPath, logger)
	if err != nil {
		return err
	}
	defer closeAdapters()
	pcRepo, pathRepo := ad.processCapacities, ad.processPaths

	// ADR 0005: every domain event is enqueued twice in the same unit of
	// work, on the integration topic and on the analytics topic
	// (ProcessCapacity events integration-only: the analytics stream is
	// plan facts only).
	encoder := outboundkafka.NewFanoutEncoder()
	register := &usecases.RegisterProcessCapacityConstraint{
		Repo: pcRepo,
		// The aggregate's ProcessCapacityRegistered/ProcessCapacityChanged
		// events are outboxed in the SAME unit of work as the save (also
		// when the labor consumer drives this use case: its Do joins the
		// consumer's open unit of work).
		Outbox: ad.outbox, Encoder: encoder, UnitOfWork: ad.uow,
	}
	pathCapacity := &usecases.GetProcessPathCapacity{
		ProcessPaths: pathRepo, ProcessCapacities: pcRepo,
		StationStandards: ad.stationStandards, Tally: ad.tallyReader,
	}
	expectedDemand := &usecases.GetExpectedDemand{Demand: ad.orderDemand}
	readiness := newReadiness(ad.pool.Ping, rt.brokers)
	server := buildServer(ad, register, pathCapacity, encoder, expectedDemand, planMetrics, readiness)
	httpServer := &http.Server{
		Addr:              httpAddr,
		Handler:           inboundhttp.NewRouter(server),
		ReadHeaderTimeout: 5 * time.Second,
	}

	// The consumers are always on; their readers dial lazily inside Run.
	consumers, closeConsumers := startConsumers(consumerSpecs(ad, rt, register, logger), rt, logger)

	// The outbox relay runs next to the consumers. It never dials Kafka at
	// boot (the writer connects lazily on its first send), so a broker
	// outage cannot crash the process. It is stopped and drained SEPARATELY
	// from the consumers above, and LAST (see serveUntilSignal): an event
	// committed by the HTTP drain or a consumer's last message must not be
	// stranded until the next pod boots.
	relayRunner, closeRelay, err := startOutboxRelay(ad.outboxStore, rt.brokers, logger)
	if err != nil {
		closeConsumers()
		return err
	}

	return serveUntilSignal(ctx, logger, httpServer, readiness, consumers, closeConsumers, relayRunner, closeRelay)
}

// buildServer assembles the REST inbound adapter's Server from the wired
// adapters/use cases. Split out of run() only to keep that function
// within the lint's length budget.
func buildServer(
	ad adapters,
	register *usecases.RegisterProcessCapacityConstraint,
	pathCapacity *usecases.GetProcessPathCapacity,
	encoder ports.EventEncoder,
	expectedDemand *usecases.GetExpectedDemand,
	planMetrics ports.PlanMetrics,
	readiness *inboundhttp.Readiness,
) *inboundhttp.Server {
	// ADR 0017: the same use case prices the plan's inbound demand and backs GET /inbound-demand.
	inboundDemand := &usecases.GetInboundDemand{Appointments: ad.inboundAppointments, Standards: ad.inboundStandards}
	return &inboundhttp.Server{
		RegisterProcessCapacityConstraint: register,
		ProcessCapacities:                 ad.processCapacities,
		RegisterProcessPath:               &usecases.RegisterProcessPath{Repo: ad.processPaths},
		GetProcessPathCapacity:            pathCapacity,
		// Phase 4: every CapacityPlan write saves the aggregate and
		// inserts its CloudEvents into the outbox in ONE UnitOfWork.
		CreateCapacityPlan: &usecases.CreateCapacityPlan{
			PathCapacity: pathCapacity,
			Plans:        ad.capacityPlans,
			Outbox:       ad.outbox,
			Encoder:      encoder,
			UnitOfWork:   ad.uow,
			Demand:       expectedDemand,
			Inbound:      inboundDemand,
			Metrics:      planMetrics,
		},
		PublishCapacityPlan: &usecases.PublishCapacityPlan{
			Plans:      ad.capacityPlans,
			Outbox:     ad.outbox,
			Encoder:    encoder,
			UnitOfWork: ad.uow,
		},
		CapacityPlans: ad.capacityPlans,

		// Station capacity (ADR 0002): operator-declared per-station
		// throughput, and the storage/station read model of a site.
		DeclareStationStandard: &usecases.DeclareStationStandard{Repo: ad.stationStandards},
		StationStandards:       ad.stationStandards,
		GetStorageCapacity:     &usecases.GetStorageCapacity{Tally: ad.tallyReader},

		// Expected demand (ADR 0004): the read model fed by the order
		// demand consumer; GET /demand and the plan's default demand read it.
		GetExpectedDemand: expectedDemand,

		// Inbound labour demand and observed capacity (ADR 0017).
		DeclareInboundLabourStandard: &usecases.DeclareInboundLabourStandard{Repo: ad.inboundStandards},
		InboundStandards:             ad.inboundStandards,
		GetInboundDemand:             inboundDemand,
		GetCapacityPlanVariance:      &usecases.GetCapacityPlanVariance{Plans: ad.capacityPlans, Observed: ad.observedCapacity},
		// Console remote list reads (REST only; no MCP tool).
		ListProcessPaths:  &usecases.ListProcessPaths{Paths: ad.pathLister},
		ListCapacityPlans: &usecases.ListCapacityPlans{Plans: ad.planLister},

		// Readiness backs GET /readyz (graceful-shutdown fix): flipped to
		// not-ready as the FIRST step of shutdown (serveUntilSignal).
		Readiness: readiness,
	}
}

// adapters is the set of outbound adapters the composition root wires.
type adapters struct {
	processCapacities ports.ProcessCapacityRepository
	processPaths      ports.ProcessPathRepository
	processedEvents   ports.ProcessedEventRepository
	storageTally      ports.StorageTallyRepository
	// tallyReader and stationStandards back the read-time station-capacity
	// composition (ADR 0002); tallyReader is the read side of storageTally.
	tallyReader      ports.StorageTallyReader
	stationStandards ports.StationStandardRepository
	// uow makes a Kafka message's claim + tally writes (and the labor
	// consumer's claim + constraint upsert) one atomic transaction; the
	// repositories above join it via ctx.
	uow ports.UnitOfWork

	// Phase 4: the CapacityPlan repository, the outbox write port and the
	// same outbox as the relay's drain source.
	capacityPlans ports.CapacityPlanRepository
	outbox        ports.OutboxRepository
	outboxStore   outboxrelay.Store

	// orderDemand is the expected-demand read model (ADR 0004), written by
	// the order demand consumer and read by GET /demand and plan creation.
	orderDemand ports.OrderDemandRepository

	// ADR 0017: the dock-appointment read model and the declared inbound
	// labour standards, and the observed-completion read model.
	inboundAppointments ports.InboundAppointmentRepository
	inboundStandards    ports.InboundStandardRepository
	observedCapacity    ports.ObservedCapacityRepository

	// pathLister and planLister are the list-read sides of processPaths and
	// capacityPlans (the same repositories), behind GET /process-paths and
	// GET /capacity-plans.
	pathLister ports.ProcessPathLister
	planLister ports.CapacityPlanLister

	// pool is the Postgres pool (the readiness check pings it).
	pool *pgxpool.Pool
}

// buildAdapters wires the Postgres adapters, running the migrations first.
// An empty databaseURL is a boot error: there is no in-memory runtime (ADR
// 0016).
func buildAdapters(ctx context.Context, databaseURL, migrationsPath string, logger *slog.Logger) (adapters, func(), error) {
	noop := func() {}

	if databaseURL == "" {
		return adapters{}, noop, errMissingDatabaseURL
	}

	// The first outbound dial of an injected pod is reset ~10s after start
	// (Istio native sidecars), so migrations and the first ping retry with
	// backoff (internal/bootretry: 5 attempts, 1+2+4+8 = 15 s of backoff);
	// on exhaustion the LAST error is returned and
	// the process still refuses to boot.
	migrationsURL := migrationsDatabaseURL(databaseURL)
	if err := bootretry.Retry(ctx, logger, "run migrations", func() error {
		return postgres.RunMigrations(migrationsURL, migrationsPath)
	}); err != nil {
		return adapters{}, noop, err
	}
	pool, err := postgres.NewPool(ctx, databaseURL)
	if err != nil {
		return adapters{}, noop, err
	}
	if err := bootretry.Retry(ctx, logger, "ping postgres", func() error { return pool.Ping(ctx) }); err != nil {
		pool.Close()
		return adapters{}, noop, err
	}
	logger.Info("postgres adapters configured", "migrations_path", migrationsPath)
	outboxRepo := postgres.NewOutboxRepo(pool)
	tallyRepo := postgres.NewStorageTallyRepo(pool)
	pathRepo, planRepo := postgres.NewProcessPathRepo(pool), postgres.NewCapacityPlanRepo(pool)

	return adapters{
		processCapacities: postgres.NewProcessCapacityRepo(pool),
		processPaths:      pathRepo,
		processedEvents:   postgres.NewProcessedEventRepo(pool),
		storageTally:      tallyRepo,
		tallyReader:       tallyRepo,
		stationStandards:  postgres.NewStationStandardRepo(pool),
		uow:               postgres.NewUnitOfWork(pool),
		capacityPlans:     planRepo,
		outbox:            outboxRepo,
		outboxStore:       outboxRepo,
		orderDemand:       postgres.NewOrderDemandRepo(pool),

		inboundAppointments: postgres.NewInboundAppointmentRepo(pool),
		inboundStandards:    postgres.NewInboundStandardRepo(pool),
		observedCapacity:    postgres.NewObservedCapacityRepo(pool),
		pathLister:          pathRepo,
		planLister:          planRepo,
		pool:                pool,
	}, pool.Close, nil
}

// migrationsDatabaseURL returns the DSN the golang-migrate boot step uses:
// MIGRATIONS_DATABASE_URL when set, else databaseURL itself. In the kind
// cluster DATABASE_URL points at PgBouncer (transaction pooling), which cannot
// honour golang-migrate's session-scoped pg_advisory_lock, so warehouse-infra
// also provisions MIGRATIONS_DATABASE_URL as a DIRECT Postgres DSN. Only the
// migration step uses it; the runtime pgxpool always uses DATABASE_URL. Where
// the split is not provisioned (local dev, CI) behaviour is unchanged.
func migrationsDatabaseURL(databaseURL string) string {
	return getenv("MIGRATIONS_DATABASE_URL", databaseURL)
}

func getenv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
