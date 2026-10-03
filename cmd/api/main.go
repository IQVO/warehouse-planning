// Command api is this service's composition root: it wires env config
// into adapters, adapters into use cases, and use cases into the HTTP
// router and Kafka consumers. This is the FIRST real composition root
// for warehouse-planning -- Phases 1/2 shipped only the REST surface and
// were wired directly from tests (features_test.go, httptest); nothing
// here existed before Phase 3.
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

	inboundhttp "github.com/claudioed/warehouse-planning/internal/adapters/inbound/http"
	inboundkafka "github.com/claudioed/warehouse-planning/internal/adapters/inbound/kafka"
	"github.com/claudioed/warehouse-planning/internal/adapters/outbound/memory"
	"github.com/claudioed/warehouse-planning/internal/adapters/outbound/postgres"
	"github.com/claudioed/warehouse-planning/internal/application/ports"
	"github.com/claudioed/warehouse-planning/internal/application/usecases"
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
	if err := run(); err != nil {
		slog.Error("service exited with error", "error", err)
		os.Exit(1)
	}
}

func run() error {
	logger := newLogger(getenv("LOG_LEVEL", "info"))
	slog.SetDefault(logger)

	httpAddr := getenv("HTTP_ADDR", ":8080")
	databaseURL := os.Getenv("DATABASE_URL")
	migrationsPath := getenv("MIGRATIONS_PATH", defaultMigrationsPath)

	ad, closeAdapters, err := buildAdapters(context.Background(), databaseURL, migrationsPath, logger)
	if err != nil {
		return err
	}
	defer closeAdapters()
	pcRepo, pathRepo := ad.processCapacities, ad.processPaths

	register := &usecases.RegisterProcessCapacityConstraint{Repo: pcRepo}
	server := &inboundhttp.Server{
		RegisterProcessCapacityConstraint: register,
		ProcessCapacities:                 pcRepo,
		RegisterProcessPath:               &usecases.RegisterProcessPath{Repo: pathRepo},
		GetProcessPathCapacity:            &usecases.GetProcessPathCapacity{ProcessPaths: pathRepo, ProcessCapacities: pcRepo},
	}
	httpServer := &http.Server{
		Addr:              httpAddr,
		Handler:           inboundhttp.NewRouter(server),
		ReadHeaderTimeout: 5 * time.Second,
	}

	consumers, closeConsumers := startKafkaConsumers(register, ad.processedEvents, ad.storageTally, ad.uow, logger)

	return serveUntilSignal(logger, httpServer, consumers, closeConsumers)
}

// adapters is the set of outbound adapters the composition root wires.
type adapters struct {
	processCapacities ports.ProcessCapacityRepository
	processPaths      ports.ProcessPathRepository
	processedEvents   ports.ProcessedEventRepository
	storageTally      ports.StorageTallyRepository
	// uow makes a Kafka message's claim + tally + constraint writes one
	// atomic transaction; all three repositories above join it via ctx.
	uow ports.UnitOfWork
}

// buildAdapters wires the Postgres adapters when DATABASE_URL is set, or
// falls back to in-memory adapters for local development without a
// database (mirroring the fleet's DATABASE_URL-empty-means-in-memory
// convention, e.g. inventory-storage's cmd/inventory).
//
// ports.ProcessPathRepository has NO Postgres implementation yet (a
// Phase 1/2 gap this phase did not set out to close) -- it is always the
// in-memory adapter here, so /process-paths' read model does not survive
// a restart even when DATABASE_URL is set. This is a known limitation,
// not an oversight: closing it is tracked as follow-up work, not part of
// Phase 3's Kafka-ingestion scope.
func buildAdapters(ctx context.Context, databaseURL, migrationsPath string, logger *slog.Logger) (adapters, func(), error) {
	noop := func() {}
	pathRepo := memory.NewProcessPathRepo()

	if databaseURL == "" {
		logger.Info("DATABASE_URL not configured; using in-memory adapters")
		pcRepo, processed, tallyRepo := memory.NewProcessCapacityRepo(), memory.NewProcessedEventRepo(), memory.NewStorageTallyRepo()
		return adapters{
			processCapacities: pcRepo,
			processPaths:      pathRepo,
			processedEvents:   processed,
			storageTally:      tallyRepo,
			// Participants make the in-memory UoW roll back on error too.
			uow: memory.NewUnitOfWork(pcRepo, processed, tallyRepo),
		}, noop, nil
	}

	if err := postgres.RunMigrations(databaseURL, migrationsPath); err != nil {
		return adapters{}, noop, err
	}
	pool, err := postgres.NewPool(ctx, databaseURL)
	if err != nil {
		return adapters{}, noop, err
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return adapters{}, noop, err
	}
	logger.Info("postgres adapters configured", "migrations_path", migrationsPath)

	return adapters{
		processCapacities: postgres.NewProcessCapacityRepo(pool),
		processPaths:      pathRepo,
		processedEvents:   postgres.NewProcessedEventRepo(pool),
		storageTally:      postgres.NewStorageTallyRepo(pool),
		uow:               postgres.NewUnitOfWork(pool),
	}, pool.Close, nil
}

// runningConsumer pairs a started Kafka consumer with the channel that
// closes once its Run goroutine has actually returned, so shutdown can
// wait on it bounded rather than firing cancel and moving on blind.
type runningConsumer struct {
	name string
	done chan struct{}
}

// startKafkaConsumers starts LaborCapacityConsumer and
// StorageCapacityConsumer as background goroutines, each only if
// KAFKA_BROKERS is set -- matching the fleet's lazy-dial pattern: a
// kafka-go Reader never dials synchronously at construction time (see
// internal/adapters/inbound/kafka/kafka.go's readerConfig doc comment),
// the real TCP connection only happens inside FetchMessage, which these
// goroutines call, so nothing here blocks process startup on Kafka being
// reachable. If KAFKA_BROKERS is unset, Kafka ingestion is simply
// disabled (logged, not fatal) -- the REST surface still works standalone.
func startKafkaConsumers(
	register *usecases.RegisterProcessCapacityConstraint,
	processedEvents ports.ProcessedEventRepository,
	storageTally ports.StorageTallyRepository,
	uow ports.UnitOfWork,
	logger *slog.Logger,
) ([]runningConsumer, func()) {
	raw := os.Getenv("KAFKA_BROKERS")
	if raw == "" {
		logger.Warn("KAFKA_BROKERS not configured; labor/storage capacity Kafka ingestion is disabled")
		return nil, func() {}
	}
	brokers := strings.Split(raw, ",")

	laborGroup := getenv("LABOR_CAPACITY_CONSUMER_GROUP", "warehouse-planning-labor-capacity")
	storageGroup := getenv("STORAGE_CAPACITY_CONSUMER_GROUP", "warehouse-planning-storage-capacity")

	laborConsumer := inboundkafka.NewLaborCapacityConsumer(brokers, laborGroup, register, processedEvents, uow, logger)
	storageConsumer := inboundkafka.NewStorageCapacityConsumer(brokers, storageGroup, register, storageTally, processedEvents, uow, logger)

	laborCtx, stopLabor := context.WithCancel(context.Background())
	storageCtx, stopStorage := context.WithCancel(context.Background())

	laborDone := make(chan struct{})
	go func() {
		defer close(laborDone)
		logger.Info("labor capacity consumer running", "topic", inboundkafka.LaborTopic, "group_id", laborGroup, "brokers", brokers)
		// Run only ever returns on error (including the plain
		// context.Canceled of an orderly shutdown), never nil. A message
		// that fails transiently does NOT end Run: it is retried with
		// backoff and its offset is committed only after it succeeds.
		if err := laborConsumer.Run(laborCtx); !errors.Is(err, context.Canceled) {
			logger.Error("labor capacity consumer stopped", "error", err)
		}
	}()

	storageDone := make(chan struct{})
	go func() {
		defer close(storageDone)
		logger.Info("storage capacity consumer running", "topic", inboundkafka.FacilityTopic, "group_id", storageGroup, "brokers", brokers)
		if err := storageConsumer.Run(storageCtx); !errors.Is(err, context.Canceled) {
			logger.Error("storage capacity consumer stopped", "error", err)
		}
	}()

	consumers := []runningConsumer{
		{name: "labor-capacity", done: laborDone},
		{name: "storage-capacity", done: storageDone},
	}
	closeFn := func() {
		stopLabor()
		stopStorage()
		_ = laborConsumer.Close()
		_ = storageConsumer.Close()
	}
	return consumers, closeFn
}

// serveUntilSignal runs httpServer until SIGINT/SIGTERM (or a listen
// error), then drains it together with every Kafka consumer, bounded by
// shutdownTimeout/consumerDrainTimeout.
func serveUntilSignal(logger *slog.Logger, httpServer *http.Server, consumers []runningConsumer, closeConsumers func()) error {
	errCh := make(chan error, 1)
	go func() {
		logger.Info("http server listening", "addr", httpServer.Addr)
		if err := httpServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			errCh <- err
		}
	}()

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	select {
	case err := <-errCh:
		closeConsumers()
		return err
	case <-ctx.Done():
	}

	shutdownCtx, cancel := context.WithTimeout(context.Background(), shutdownTimeout)
	defer cancel()
	err := httpServer.Shutdown(shutdownCtx)

	closeConsumers()
	for _, c := range consumers {
		select {
		case <-c.done:
		case <-time.After(consumerDrainTimeout):
			logger.Warn("kafka consumer did not stop before the shutdown drain deadline", "consumer", c.name)
		}
	}

	return err
}

// newLogger builds the process-wide structured logger. LOG_LEVEL maps
// debug|info|warn|error (case-insensitive) to the matching slog.Level,
// defaulting to Info for unset/unrecognized values.
func newLogger(level string) *slog.Logger {
	var lvl slog.Level
	switch strings.ToLower(level) {
	case "debug":
		lvl = slog.LevelDebug
	case "warn":
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
