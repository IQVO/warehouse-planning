//go:build integration

package kafka_test

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/testcontainers/testcontainers-go"
	tckafka "github.com/testcontainers/testcontainers-go/modules/kafka"
	tcpostgres "github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/wait"

	kafkaconsumer "github.com/claudioed/warehouse-planning/internal/adapters/inbound/kafka"
	"github.com/claudioed/warehouse-planning/internal/adapters/outbound/postgres"
)

// This file owns the package-wide Kafka broker lifecycle for every
// *_integration_test.go in this package (TestMain runs once per test
// binary). A real broker is started via testcontainers-go -- never a
// skip-gated external one, never hardcoded localhost:9092 (see
// internal/architecture/fitness_test.go's
// TestKafkaIntegrationTestsUseTestcontainers and HARNESS.md). One broker
// is shared across every test; isolation comes from each test using its
// own unique topic.

var (
	sharedBrokers   []string
	sharedContainer testcontainers.Container
)

func TestMain(m *testing.M) {
	code := m.Run()
	if sharedContainer != nil {
		if err := testcontainers.TerminateContainer(sharedContainer); err != nil {
			fmt.Fprintf(os.Stderr, "terminate kafka container: %v\n", err)
		}
	}
	os.Exit(code)
}

func startKafkaBroker(t *testing.T) []string {
	t.Helper()
	if sharedBrokers != nil {
		return sharedBrokers
	}

	ctx := context.Background()
	container, err := tckafka.Run(ctx, "confluentinc/confluent-local:7.6.1",
		tckafka.WithClusterID("warehouse-planning-itest"),
	)
	if err != nil {
		t.Fatalf("start kafka container: %v", err)
	}
	sharedContainer = container

	brokers, err := container.Brokers(ctx)
	if err != nil {
		t.Fatalf("resolve kafka brokers: %v", err)
	}
	sharedBrokers = brokers
	return brokers
}

// uniqueTopic gives each test its own topic so tests never contaminate
// one another through the shared broker.
func uniqueTopic(prefix string) string {
	return fmt.Sprintf("%s.itest-%d", prefix, time.Now().UnixNano())
}

// uniqueGroupID mirrors this phase's env-driven consumer-group
// convention (see labor_capacity_consumer.go/storage_capacity_consumer.go
// doc comments) with a per-test-run unique suffix so tests never share a
// committed offset with one another.
func uniqueGroupID(prefix string) string {
	return fmt.Sprintf("%s-itest-%d", prefix, time.Now().UnixNano())
}

func runLaborConsumerInBackground(t *testing.T, c *kafkaconsumer.LaborCapacityConsumer) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	go func() { _ = c.Run(ctx) }()
}

func runStorageConsumerInBackground(t *testing.T, c *kafkaconsumer.StorageCapacityConsumer) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	go func() { _ = c.Run(ctx) }()
}

// migrationsDir resolves this service's migrations/ directory relative
// to this test file, so startPostgresPool works regardless of the
// working directory `go test` is invoked from.
func migrationsDir(t *testing.T) string {
	t.Helper()
	_, thisFile, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("unable to resolve test file path")
	}
	return filepath.Join(filepath.Dir(thisFile), "..", "..", "outbound", "postgres", "migrations")
}

// startPostgresPool boots a real Postgres via testcontainers-go (never a
// skip-gated external DB), runs every migration, and returns an open
// pgxpool.Pool -- the SAME outbound adapter Phase 1's REST surface and
// this phase's Kafka consumers both write through.
func startPostgresPool(t *testing.T) (string, *pgxpool.Pool) {
	t.Helper()
	ctx := context.Background()

	container, err := tcpostgres.Run(ctx,
		"postgres:16-alpine",
		tcpostgres.WithDatabase("warehouse_planning_test"),
		tcpostgres.WithUsername("warehouse_planning_test"),
		tcpostgres.WithPassword("warehouse_planning_test"),
		testcontainers.WithWaitStrategy(
			wait.ForLog("database system is ready to accept connections").WithOccurrence(2).WithStartupTimeout(60*time.Second),
		),
	)
	if err != nil {
		t.Fatalf("start postgres container: %v", err)
	}
	t.Cleanup(func() {
		if err := container.Terminate(context.Background()); err != nil {
			t.Logf("terminate postgres container: %v", err)
		}
	})

	databaseURL, err := container.ConnectionString(ctx, "sslmode=disable")
	if err != nil {
		t.Fatalf("resolve postgres connection string: %v", err)
	}

	if err := postgres.RunMigrations(databaseURL, migrationsDir(t)); err != nil {
		t.Fatalf("run migrations: %v", err)
	}

	pool, err := postgres.NewPool(ctx, databaseURL)
	if err != nil {
		t.Fatalf("open pool: %v", err)
	}
	t.Cleanup(pool.Close)

	return databaseURL, pool
}
