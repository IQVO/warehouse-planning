//go:build integration

package main

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"net"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"runtime"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/google/uuid"
	kafkago "github.com/segmentio/kafka-go"
	"github.com/testcontainers/testcontainers-go"
	tckafka "github.com/testcontainers/testcontainers-go/modules/kafka"
	tcpostgres "github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/wait"

	"github.com/claudioed/warehouse-planning/internal/adapters/outbound/analyticsstore"
	outboundkafka "github.com/claudioed/warehouse-planning/internal/adapters/outbound/kafka"
	"github.com/claudioed/warehouse-planning/internal/domain/capacityplan"
	"github.com/claudioed/warehouse-planning/internal/domain/processcapacity"
)

// The real binary against real infrastructure, all started with
// testcontainers (never a skip-gate, never a hardcoded broker): run() boots
// on an EMPTY analytical Postgres and applies the service's OWN analytics
// migrations (ADR 0005 §4), the consumer joins the env-supplied group on the
// production analytics topic and replays it from the earliest offset
// (kafka-go's default StartOffset for a brand-new group, ADR 0005 §4:
// "a brand-new group starts at the earliest offset so the model can be
// rebuilt from retained history"), folding the four events the repo's own
// analytics encoder produced into the documented plan_facts row, and a
// SIGTERM drains the process cleanly (readiness flips, the in-flight message
// finishes, exit 0).

var (
	sharedMu        sync.Mutex
	sharedBrokers   []string
	sharedKafka     testcontainers.Container
	sharedPostgres  testcontainers.Container
	sharedAnalytics string // DSN of the EMPTY analytical database
)

func TestMain(m *testing.M) {
	code := m.Run()
	sharedMu.Lock()
	kafka, postgres := sharedKafka, sharedPostgres
	sharedMu.Unlock()
	if postgres != nil {
		if err := testcontainers.TerminateContainer(postgres); err != nil {
			fmt.Fprintf(os.Stderr, "terminate postgres container: %v\n", err)
		}
	}
	if kafka != nil {
		if err := testcontainers.TerminateContainer(kafka); err != nil {
			fmt.Fprintf(os.Stderr, "terminate kafka container: %v\n", err)
		}
	}
	os.Exit(code)
}

// startEmptyAnalyticsPostgres boots the analytical database with NO
// migrations applied: applying analytics/migrations is the projector's own
// boot step, which is exactly what is under test.
func startEmptyAnalyticsPostgres(t *testing.T) string {
	t.Helper()
	sharedMu.Lock()
	defer sharedMu.Unlock()
	if sharedAnalytics != "" {
		return sharedAnalytics
	}
	ctx := context.Background()
	c, err := tcpostgres.Run(ctx, "postgres:16-alpine",
		tcpostgres.WithDatabase("warehouse_planning_analytics"),
		tcpostgres.WithUsername("itest"), tcpostgres.WithPassword("itest"),
		testcontainers.WithWaitStrategy(wait.ForLog("database system is ready to accept connections").WithOccurrence(2).WithStartupTimeout(60*time.Second)))
	if err != nil {
		t.Fatalf("start empty analytics postgres: %v", err)
	}
	sharedPostgres = c
	url, err := c.ConnectionString(ctx, "sslmode=disable")
	if err != nil {
		t.Fatalf("analytics dsn: %v", err)
	}
	sharedAnalytics = url
	return url
}

// startKafkaBroker is the package-wide broker (one per test binary, the
// pattern of internal/adapters/inbound/kafka/main_integration_test.go).
func startKafkaBroker(t *testing.T) []string {
	t.Helper()
	sharedMu.Lock()
	defer sharedMu.Unlock()
	if sharedBrokers != nil {
		return sharedBrokers
	}
	ctx := context.Background()
	c, err := tckafka.Run(ctx, "confluentinc/confluent-local:7.6.1", tckafka.WithClusterID("planning-projector-itest"))
	if err != nil {
		t.Fatalf("start kafka: %v", err)
	}
	sharedKafka = c
	brokers, err := c.Brokers(ctx)
	if err != nil {
		t.Fatalf("kafka brokers: %v", err)
	}
	sharedBrokers = brokers
	return brokers
}

// createTopic creates the topic explicitly and waits for its leader, the
// fleet rule (never rely on auto-create before the first read).
func createTopic(t *testing.T, broker, topic string) {
	t.Helper()
	conn, err := kafkago.Dial("tcp", broker)
	if err != nil {
		t.Fatalf("dial broker: %v", err)
	}
	defer conn.Close()
	if err := conn.CreateTopics(kafkago.TopicConfig{Topic: topic, NumPartitions: 1, ReplicationFactor: 1}); err != nil {
		t.Fatalf("create topic: %v", err)
	}
	for deadline := time.Now().Add(30 * time.Second); time.Now().Before(deadline); time.Sleep(200 * time.Millisecond) {
		if parts, err := conn.ReadPartitions(topic); err == nil && len(parts) == 1 && parts[0].Leader.ID != 0 {
			return
		}
	}
	t.Fatalf("topic %s never got a leader", topic)
}

// freeAddr reserves an ephemeral loopback port for the admin server.
func freeAddr(t *testing.T) string {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = l.Close() }()
	return l.Addr().String()
}

// ownSignal keeps a SIGTERM handler registered from before run() starts, so
// the test's stop signal can never kill the test binary itself in the window
// before run()'s signal.NotifyContext attaches, and delivers it repeatedly
// until run() returns.
func ownSignal() chan os.Signal {
	sig := make(chan os.Signal, 8)
	signal.Notify(sig, syscall.SIGTERM)
	return sig
}

func stopWithSIGTERM(t *testing.T, done <-chan error) error {
	t.Helper()
	for deadline := time.Now().Add(30 * time.Second); time.Now().Before(deadline); time.Sleep(200 * time.Millisecond) {
		_ = syscall.Kill(os.Getpid(), syscall.SIGTERM)
		select {
		case err := <-done:
			return err
		case <-time.After(300 * time.Millisecond):
		}
	}
	t.Fatal("the projector did not stop after SIGTERM")
	return nil
}

func waitUntil(t *testing.T, what string, cond func() bool) {
	t.Helper()
	for deadline := time.Now().Add(90 * time.Second); time.Now().Before(deadline); time.Sleep(200 * time.Millisecond) {
		if cond() {
			return
		}
	}
	t.Fatalf("timed out waiting for %s", what)
}

// TestProjectorRunMigratesProjectsAndDrainsCleanly boots the real run():
// empty database -> own migrations -> /readyz -> the four encoder-produced
// plan events projected -> SIGTERM -> nil.
func TestProjectorRunMigratesProjectsAndDrainsCleanly(t *testing.T) {
	analyticsURL := startEmptyAnalyticsPostgres(t)
	brokers := startKafkaBroker(t)
	createTopic(t, brokers[0], outboundkafka.AnalyticsTopic)

	// The repo's own encoder turns the domain events into the analytics
	// CloudEvents (the "section 43" worked example: demand 12000, capacity
	// 8000/order-window, shortage 4000 on the REBIN step, bound by LABOR).
	created := time.Date(2026, 11, 13, 8, 0, 0, 0, time.UTC)
	published := created.Add(10 * time.Minute)
	windowStart, windowEnd := time.Date(2026, 11, 13, 8, 0, 0, 0, time.UTC), time.Date(2026, 11, 13, 16, 0, 0, 0, time.UTC)
	const (
		planID      = "PLAN-PROJECTOR-ITEST-1"
		warehouseID = "WH-PROJECTOR-ITEST"
		location    = "PATH-ZONE-PROJECTOR"
	)
	head := func(at time.Time) capacityplan.Header { return capacityplan.Header{PlanID: planID, At: at} }
	msgs, err := (&outboundkafka.AnalyticsEncoder{NewID: uuid.NewString}).EncodeCapacityPlanEvents(
		capacityplan.CapacityPlanCreated{Header: head(created), WarehouseID: warehouseID, Location: location,
			PathID: "pick-rebin-pack", WindowStart: windowStart, WindowEnd: windowEnd,
			AssignedDemand: 12000, PathCapacity: 2500, CapacityOverWindow: 8000, Shortage: 4000,
			BottleneckStep: processcapacity.ProcessType("REBIN")},
		capacityplan.CapacityPlanPublished{Header: head(published), WarehouseID: warehouseID, Location: location,
			PathID: "pick-rebin-pack", WindowStart: windowStart, WindowEnd: windowEnd,
			AssignedDemand: 12000, PathCapacity: 2500, CapacityOverWindow: 8000, Shortage: 4000,
			BottleneckStep: processcapacity.ProcessType("REBIN"), BottleneckConstraint: processcapacity.ConstraintLabor},
		capacityplan.CapacityShortageDetected{Header: head(published), WarehouseID: warehouseID, Location: location,
			PathID: "pick-rebin-pack", WindowStart: windowStart, WindowEnd: windowEnd,
			AssignedDemand: 12000, CapacityOverWindow: 8000, Shortage: 4000, BottleneckStep: processcapacity.ProcessType("REBIN")},
		capacityplan.BottleneckDetected{Header: head(published), WarehouseID: warehouseID, Location: location,
			PathID: "pick-rebin-pack", WindowStart: windowStart, WindowEnd: windowEnd,
			BottleneckStep: processcapacity.ProcessType("REBIN"), PathCapacity: 2500},
	)
	if err != nil {
		t.Fatalf("encode the plan's analytics events: %v", err)
	}
	producer := &kafkago.Writer{Addr: kafkago.TCP(brokers...), Topic: outboundkafka.AnalyticsTopic, BatchTimeout: 10 * time.Millisecond}
	t.Cleanup(func() { _ = producer.Close() })
	out := make([]kafkago.Message, len(msgs))
	for i, m := range msgs {
		if m.Topic != outboundkafka.AnalyticsTopic {
			t.Fatalf("encoder routed message %d to %s, want the analytics topic", i, m.Topic)
		}
		out[i] = kafkago.Message{Key: m.Key, Value: m.Value}
	}
	if err := producer.WriteMessages(context.Background(), out...); err != nil {
		t.Fatal(err)
	}

	_, thisFile, _, _ := runtime.Caller(0)
	t.Setenv("ANALYTICS_DATABASE_URL", analyticsURL)
	t.Setenv("KAFKA_BROKERS", brokers[0])
	t.Setenv("ANALYTICS_CONSUMER_GROUP", fmt.Sprintf("planning-projector-itest-%d", time.Now().UnixNano()))
	t.Setenv("ANALYTICS_MIGRATIONS_PATH", filepath.Join(filepath.Dir(thisFile), "..", "..", "analytics", "migrations"))
	t.Setenv("ADMIN_ADDR", freeAddr(t))
	t.Setenv("LOG_LEVEL", "error")

	sig := ownSignal()
	defer signal.Stop(sig)
	done := make(chan error, 1)
	go func() { done <- run() }()

	// The documented boot probe: liveness always 200, readiness 200 only
	// once the analytical Postgres and the broker answer (the admin mux's
	// two named dependencies).
	admin := "http://" + os.Getenv("ADMIN_ADDR")
	waitUntil(t, "/healthz 200", func() bool {
		resp, err := http.Get(admin + "/healthz")
		if err != nil {
			return false
		}
		_ = resp.Body.Close()
		return resp.StatusCode == http.StatusOK
	})
	waitUntil(t, "/readyz 200", func() bool {
		resp, err := http.Get(admin + "/readyz")
		if err != nil {
			return false
		}
		_ = resp.Body.Close()
		return resp.StatusCode == http.StatusOK
	})

	// The documented projection (ADR 0005 §1): one plan_facts row assembled
	// from the four events, each event upserting only the columns it
	// carries; created_at/published_at are the CloudEvents times; the
	// analytics-only binding_constraint survived the encoder.
	reader, err := analyticsstore.NewReadOnlyPool(context.Background(), analyticsURL)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(reader.Close)
	var (
		gotWarehouse, gotLocation, step, constraint string
		shortage, gotCreated, gotPublished          sql.NullFloat64
	)
	waitUntil(t, "the plan projected into plan_facts", func() bool {
		err := reader.QueryRow(context.Background(), `
			SELECT warehouse_id, location, bottleneck_step, binding_constraint, shortage,
			       extract(epoch from created_at), extract(epoch from published_at)
			FROM plan_facts WHERE plan_id = $1`, planID).
			Scan(&gotWarehouse, &gotLocation, &step, &constraint, &shortage, &gotCreated, &gotPublished)
		return err == nil
	})
	if gotWarehouse != warehouseID || gotLocation != location {
		t.Errorf("plan_facts site = %s/%s, want %s/%s", gotWarehouse, gotLocation, warehouseID, location)
	}
	if step != "REBIN" || constraint != "LABOR" {
		t.Errorf("plan_facts bottleneck = %s bound by %s, want REBIN bound by LABOR", step, constraint)
	}
	if !shortage.Valid || shortage.Float64 != 4000 {
		t.Errorf("plan_facts shortage = %v, want 4000", shortage)
	}
	if !gotCreated.Valid || gotCreated.Float64 != float64(created.Unix()) {
		t.Errorf("plan_facts created_at = %v, want %v", gotCreated, created)
	}
	if !gotPublished.Valid || gotPublished.Float64 != float64(published.Unix()) {
		t.Errorf("plan_facts published_at = %v, want %v", gotPublished, published)
	}
	// Idempotency ledger: exactly the four CloudEvents ids, one row each.
	waitUntil(t, "four processed events", func() bool {
		var n int
		return reader.QueryRow(context.Background(),
			`SELECT count(*) FROM analytics_processed_events`).Scan(&n) == nil && n == 4
	})

	if err := stopWithSIGTERM(t, done); err != nil && !errors.Is(err, context.Canceled) {
		t.Fatalf("a SIGTERM'd projector must drain cleanly, got %v", err)
	}
}
