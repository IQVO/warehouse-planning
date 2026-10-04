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
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	inboundhttp "github.com/claudioed/warehouse-planning/internal/adapters/inbound/http"
	inboundkafka "github.com/claudioed/warehouse-planning/internal/adapters/inbound/kafka"
	outboundkafka "github.com/claudioed/warehouse-planning/internal/adapters/outbound/kafka"
	"github.com/claudioed/warehouse-planning/internal/adapters/outbound/memory"
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
	pathCapacity := &usecases.GetProcessPathCapacity{ProcessPaths: pathRepo, ProcessCapacities: pcRepo}
	encoder := outboundkafka.NewEncoder()
	server := &inboundhttp.Server{
		RegisterProcessCapacityConstraint: register,
		ProcessCapacities:                 pcRepo,
		RegisterProcessPath:               &usecases.RegisterProcessPath{Repo: pathRepo},
		GetProcessPathCapacity:            pathCapacity,
		// Phase 4: every CapacityPlan write saves the aggregate and
		// inserts its CloudEvents into the outbox in ONE UnitOfWork.
		CreateCapacityPlan: &usecases.CreateCapacityPlan{
			PathCapacity: pathCapacity,
			Plans:        ad.capacityPlans,
			Outbox:       ad.outbox,
			Encoder:      encoder,
			UnitOfWork:   ad.uow,
		},
		PublishCapacityPlan: &usecases.PublishCapacityPlan{
			Plans:      ad.capacityPlans,
			Outbox:     ad.outbox,
			Encoder:    encoder,
			UnitOfWork: ad.uow,
		},
		CapacityPlans: ad.capacityPlans,
	}
	httpServer := &http.Server{
		Addr:              httpAddr,
		Handler:           inboundhttp.NewRouter(server),
		ReadHeaderTimeout: 5 * time.Second,
	}

	consumers, closeConsumers := startKafkaConsumers(register, ad.processedEvents, ad.storageTally, ad.uow, logger)

	// The outbox relay runs next to the consumers. It never dials Kafka at
	// boot (the writer connects lazily on its first send), so a broker
	// outage cannot crash the process.
	relayRunner, closeRelay, err := startOutboxRelay(ad.outboxStore, logger)
	if err != nil {
		closeConsumers()
		return err
	}
	consumers = append(consumers, relayRunner)

	return serveUntilSignal(logger, httpServer, consumers, func() { closeConsumers(); closeRelay() })
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

	// Phase 4: the CapacityPlan repository, the outbox write port and the
	// same outbox as the relay's drain source.
	capacityPlans ports.CapacityPlanRepository
	outbox        ports.OutboxRepository
	outboxStore   outboxrelay.Store
}

// buildAdapters wires the Postgres adapters when DATABASE_URL is set, or
// falls back to in-memory adapters for local development without a
// database (mirroring the fleet's DATABASE_URL-empty-means-in-memory
// convention, e.g. inventory-storage's cmd/inventory).
//
// ProcessPaths persist in Postgres (migration 0004) when DATABASE_URL is
// set, so declared paths survive a pod restart; without a database the
// in-memory repo is used exactly as before.
func buildAdapters(ctx context.Context, databaseURL, migrationsPath string, logger *slog.Logger) (adapters, func(), error) {
	noop := func() {}

	if databaseURL == "" {
		logger.Info("DATABASE_URL not configured; using in-memory adapters")
		pathRepo := memory.NewProcessPathRepo()
		pcRepo, processed, tallyRepo := memory.NewProcessCapacityRepo(), memory.NewProcessedEventRepo(), memory.NewStorageTallyRepo()
		planRepo, outboxRepo := memory.NewCapacityPlanRepo(), memory.NewOutboxRepo()
		return adapters{
			processCapacities: pcRepo,
			processPaths:      pathRepo,
			processedEvents:   processed,
			storageTally:      tallyRepo,
			// Participants make the in-memory UoW roll back on error too.
			uow:           memory.NewUnitOfWork(pcRepo, processed, tallyRepo, planRepo, outboxRepo),
			capacityPlans: planRepo,
			outbox:        outboxRepo,
			outboxStore:   outboxRepo,
		}, noop, nil
	}

	// The first outbound dial of an injected pod is reset ~10s after start
	// (Istio native sidecars), so migrations and the first ping retry with
	// backoff (~31s budget); on exhaustion the LAST error is returned and
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

	return adapters{
		processCapacities: postgres.NewProcessCapacityRepo(pool),
		processPaths:      postgres.NewProcessPathRepo(pool),
		processedEvents:   postgres.NewProcessedEventRepo(pool),
		storageTally:      postgres.NewStorageTallyRepo(pool),
		uow:               postgres.NewUnitOfWork(pool),
		capacityPlans:     postgres.NewCapacityPlanRepo(pool),
		outbox:            outboxRepo,
		outboxStore:       outboxRepo,
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

// Event publisher modes (EVENT_PUBLISHER).
const (
	publisherLog   = "log"
	publisherKafka = "kafka"
)

// parsePublisherMode validates EVENT_PUBLISHER: "" and "log" select the
// broker-free log sink (the default, so tests and local dev need no
// Kafka), "kafka" the real relay sink; anything else is a config error.
func parsePublisherMode(raw string) (string, error) {
	switch raw {
	case "", publisherLog:
		return publisherLog, nil
	case publisherKafka:
		return publisherKafka, nil
	default:
		return "", fmt.Errorf("unknown EVENT_PUBLISHER %q (want kafka or log)", raw)
	}
}

// parseRelayInterval reads OUTBOX_RELAY_INTERVAL (a Go duration such as
// "500ms" or "2s", or plain seconds), defaulting to 1s when unset,
// malformed or non-positive -- the interval is a tuning knob, never a
// reason to refuse to boot.
func parseRelayInterval(raw string, logger *slog.Logger) time.Duration {
	if raw == "" {
		return outboxrelay.DefaultInterval
	}
	d, err := time.ParseDuration(raw)
	if err != nil {
		if secs, serr := strconv.ParseFloat(raw, 64); serr == nil {
			d, err = time.Duration(secs*float64(time.Second)), nil
		}
	}
	if err != nil || d <= 0 {
		logger.Warn("invalid OUTBOX_RELAY_INTERVAL; using the default", "value", raw, "default", outboxrelay.DefaultInterval)
		return outboxrelay.DefaultInterval
	}
	return d
}

// startOutboxRelay starts the outbox relay goroutine. EVENT_PUBLISHER
// picks the sink: kafka (needs KAFKA_BROKERS) or log (default). Kafka is
// dialled lazily inside the relay loop on the first send, never here.
func startOutboxRelay(store outboxrelay.Store, logger *slog.Logger) (runningConsumer, func(), error) {
	mode, err := parsePublisherMode(os.Getenv("EVENT_PUBLISHER"))
	if err != nil {
		return runningConsumer{}, nil, err
	}

	var (
		sink      outboxrelay.Sink = outboxrelay.LogSink{Logger: logger}
		closeSink                  = func() {}
	)
	if mode == publisherKafka {
		raw := os.Getenv("KAFKA_BROKERS")
		if raw == "" {
			return runningConsumer{}, nil, errors.New("EVENT_PUBLISHER=kafka requires KAFKA_BROKERS")
		}
		kafkaSink := outboundkafka.NewRelaySink(strings.Split(raw, ","))
		sink = kafkaSink
		closeSink = func() {
			if err := kafkaSink.Close(); err != nil {
				logger.Error("kafka relay sink close failed", "error", err)
			}
		}
	}

	interval := parseRelayInterval(os.Getenv("OUTBOX_RELAY_INTERVAL"), logger)
	relay := outboxrelay.NewRelay(store, sink, logger, outboxrelay.WithInterval(interval))

	ctx, stop := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		defer closeSink()
		logger.Info("outbox relay running", "publisher", mode, "interval", interval, "topic", outboundkafka.Topic)
		if err := relay.Run(ctx); !errors.Is(err, context.Canceled) {
			logger.Error("outbox relay stopped", "error", err)
		}
	}()
	return runningConsumer{name: "outbox-relay", done: done}, stop, nil
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
