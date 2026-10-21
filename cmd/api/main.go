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
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	inboundhttp "github.com/claudioed/warehouse-planning/internal/adapters/inbound/http"
	outboundkafka "github.com/claudioed/warehouse-planning/internal/adapters/outbound/kafka"
	outboxrelay "github.com/claudioed/warehouse-planning/internal/adapters/outbound/outbox"
	"github.com/claudioed/warehouse-planning/internal/adapters/outbound/postgres"
	"github.com/claudioed/warehouse-planning/internal/adapters/outbound/telemetry"
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

// serveUntilSignal runs httpServer until SIGINT/SIGTERM (or a listen
// error), then performs the fleet's graceful-shutdown sequence (docs/adr
// on standard metrics / readiness, mirroring order-management's
// drainUnderShutdown and fulfillment-execution's waitForShutdown, the
// fleet references for this phase):
//
//  1. readiness.SetNotReady() -- /readyz starts answering 503 so a
//     Kubernetes readinessProbe stops routing NEW traffic here;
//  2. wait shutdownDrainDelay() -- gives the probe/endpoint controller a
//     window to observe the flip BEFORE the listener closes;
//  3. httpServer.Shutdown -- drain in-flight HTTP requests (one outbox
//     writer);
//  4. stop and await the Kafka consumers (the other outbox writer),
//     including the commit of any message they are mid-handling;
//  5. stop and await the outbox relay LAST -- only after every writer
//     above has stopped, so an event committed by the HTTP drain or a
//     consumer's last message is not stranded until the next pod boots;
//  6. the caller's deferred closeAdapters() (pool.Close) runs after this
//     function returns, i.e. after every step above.
//
// Steps 3-5 share shutdownTimeout/consumerDrainTimeout; the drain delay is
// additional (terminationGracePeriodSeconds must cover both).
//
// The process also exits non-zero when the relay or a consumer returns on its
// own (a lost broker, a failed reader): the service cannot run without them
// and must not sit "alive" with a dead worker, so the orchestrator restarts
// it.
func serveUntilSignal(
	parent context.Context,
	logger *slog.Logger,
	httpServer *http.Server,
	readiness *inboundhttp.Readiness,
	consumers []*worker,
	closeConsumers func(),
	relay *worker,
	closeRelay func(),
) error {
	errCh := make(chan error, 1)
	go func() {
		logger.Info("http server listening", "addr", httpServer.Addr)
		if err := httpServer.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			errCh <- err
		}
	}()

	ctx, stop := signal.NotifyContext(parent, syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	died := watchWorkers(append([]*worker{relay}, consumers...)...)

	select {
	case err := <-errCh:
		closeConsumers()
		closeRelay()
		return err
	case name := <-died:
		closeConsumers()
		closeRelay()
		_ = httpServer.Close()
		return fmt.Errorf("%s stopped unexpectedly: exiting so the orchestrator restarts the service", name)
	case <-ctx.Done():
	}

	readiness.SetNotReady()
	if delay := shutdownDrainDelay(logger); delay > 0 {
		logger.Info("shutdown: readiness flipped to not-ready; waiting for traffic to drain", "drain_delay", delay.String())
		time.Sleep(delay)
	}

	shutdownCtx, cancel := context.WithTimeout(context.Background(), shutdownTimeout)
	defer cancel()
	err := httpServer.Shutdown(shutdownCtx)

	closeConsumers()
	for _, c := range consumers {
		await(logger, c)
	}

	// Every outbox writer has now stopped: let the relay finish its
	// in-flight pass so an event committed by the last HTTP request or a
	// consumer's last message is not stranded until the next pod boots.
	closeRelay()
	await(logger, relay)

	return err
}

// watchWorkers returns a channel that receives the name of every worker that
// returns. Workers only return when stopped or on an unrecoverable failure, so
// a value read while the service is serving means a worker died.
func watchWorkers(workers ...*worker) <-chan string {
	died := make(chan string, len(workers))
	for _, w := range workers {
		if w == nil {
			continue
		}
		go func() {
			<-w.done
			died <- w.name
		}()
	}
	return died
}

// await waits (bounded) for w to return; a nil w is a no-op.
func await(logger *slog.Logger, w *worker) {
	if w == nil {
		return
	}
	select {
	case <-w.done:
	case <-time.After(consumerDrainTimeout):
		logger.Warn("worker did not stop before the shutdown drain deadline", "worker", w.name)
	}
}

// DefaultShutdownDrainDelay is how long shutdown waits, after flipping
// /readyz to not-ready and before closing the HTTP listener, for
// Kubernetes' readinessProbe and the endpoint controller to observe the
// flip and stop routing NEW traffic to this pod. SHUTDOWN_DRAIN_DELAY
// overrides it; "0" disables the wait (tests, local dev).
const DefaultShutdownDrainDelay = 5 * time.Second

// shutdownDrainDelay reads SHUTDOWN_DRAIN_DELAY. Unlike getenv's fallback
// convention, "0" is a legal value (disable the delay); an unset,
// negative or unparsable value falls back to DefaultShutdownDrainDelay.
func shutdownDrainDelay(logger *slog.Logger) time.Duration {
	raw := os.Getenv("SHUTDOWN_DRAIN_DELAY")
	if raw == "" {
		return DefaultShutdownDrainDelay
	}
	d, err := time.ParseDuration(raw)
	if err != nil || d < 0 {
		logger.Warn("ignoring invalid SHUTDOWN_DRAIN_DELAY", "value", raw, "default", DefaultShutdownDrainDelay.String())
		return DefaultShutdownDrainDelay
	}
	return d
}

// serviceVersion is this binary's OTel service.version resource
// attribute. SERVICE_VERSION is set by the chart from the image tag; a
// local run without it reports "dev".
func serviceVersion() string {
	return getenv("SERVICE_VERSION", "dev")
}

// setupTelemetry installs the standard-metrics adapter (docs/adr/0011):
// a TracerProvider + MeterProvider exporting OTLP to an optional
// Collector (Setup never blocks on the Collector being reachable -- a
// missing one degrades to "telemetry dropped", never to "service won't
// start"), and registers the Tier-2 plan-created counter. The returned
// closer flushes both providers on a bounded timeout and logs (never
// returns) a shutdown failure; call it via defer.
func setupTelemetry(logger *slog.Logger) (closer func(), metrics *telemetry.PlanMetrics, err error) {
	otelCtx, otelCancel := context.WithTimeout(context.Background(), 10*time.Second)
	otelShutdown, err := telemetry.Setup(otelCtx, inboundhttp.DefaultServiceName, serviceVersion(), getenv("OTEL_EXPORTER_OTLP_ENDPOINT", telemetry.DefaultOTLPEndpoint))
	otelCancel()
	if err != nil {
		return nil, nil, fmt.Errorf("telemetry setup: %w", err)
	}
	closer = func() {
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := otelShutdown(shutdownCtx); err != nil {
			logger.Error("telemetry shutdown failed", "error", err)
		}
	}
	metrics, err = telemetry.NewPlanMetrics()
	if err != nil {
		return nil, nil, fmt.Errorf("telemetry: register plan metrics: %w", err)
	}
	return closer, metrics, nil
}

// newLogger builds the process-wide structured logger. LOG_LEVEL maps
// debug|info|warn|warning|error (case-insensitive; "warning" is accepted
// like "warn" fleet-wide, gap fix D22) to the matching slog.Level,
// defaulting to Info for unset/unrecognized values.
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
