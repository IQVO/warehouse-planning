//go:build integration

package outbox_test

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"runtime"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	kafkago "github.com/segmentio/kafka-go"
	"github.com/testcontainers/testcontainers-go"
	tckafka "github.com/testcontainers/testcontainers-go/modules/kafka"
	tcpostgres "github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/wait"

	"github.com/claudioed/warehouse-planning/internal/adapters/kafka/cloudevents"
	outboundkafka "github.com/claudioed/warehouse-planning/internal/adapters/outbound/kafka"
	"github.com/claudioed/warehouse-planning/internal/adapters/outbound/memory"
	relay "github.com/claudioed/warehouse-planning/internal/adapters/outbound/outbox"
	"github.com/claudioed/warehouse-planning/internal/adapters/outbound/postgres"
	"github.com/claudioed/warehouse-planning/internal/application/outbox"
	"github.com/claudioed/warehouse-planning/internal/application/usecases"
	"github.com/claudioed/warehouse-planning/internal/domain/processcapacity"
	"github.com/claudioed/warehouse-planning/internal/domain/processpath"
)

// This test drives the WHOLE Phase 4 publish path against real
// infrastructure, both started with testcontainers (never skip-gated, never
// a hardcoded broker address): a plan with a shortage is created and
// published in real Postgres, the relay drains the outbox to a real Kafka
// broker on a unique topic, and the four CloudEvents are consumed back.

// One broker for the whole package (containers take seconds to boot);
// isolation comes from every test using its own unique topic.
var (
	kafkaOnce       sync.Once
	sharedBrokers   []string
	sharedContainer testcontainers.Container
	startKafkaErr   error
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

func startKafka(t *testing.T) []string {
	t.Helper()
	kafkaOnce.Do(func() {
		ctx := context.Background()
		container, err := tckafka.Run(ctx, "confluentinc/confluent-local:7.6.1", tckafka.WithClusterID("warehouse-planning-relay-itest"))
		if err != nil {
			startKafkaErr = err
			return
		}
		sharedContainer = container
		sharedBrokers, startKafkaErr = container.Brokers(ctx)
	})
	if startKafkaErr != nil {
		t.Fatalf("start kafka container: %v", startKafkaErr)
	}
	return sharedBrokers
}

func startPostgresPool(t *testing.T) *pgxpool.Pool {
	t.Helper()
	ctx := context.Background()
	container, err := tcpostgres.Run(ctx, "postgres:16-alpine",
		tcpostgres.WithDatabase("warehouse_planning_test"),
		tcpostgres.WithUsername("warehouse_planning_test"),
		tcpostgres.WithPassword("warehouse_planning_test"),
		testcontainers.WithWaitStrategy(wait.ForLog("database system is ready to accept connections").WithOccurrence(2).WithStartupTimeout(60*time.Second)),
	)
	if err != nil {
		t.Fatalf("start postgres container: %v", err)
	}
	t.Cleanup(func() { _ = container.Terminate(context.Background()) })
	url, err := container.ConnectionString(ctx, "sslmode=disable")
	if err != nil {
		t.Fatalf("connection string: %v", err)
	}
	_, thisFile, _, _ := runtime.Caller(0)
	migrations := filepath.Join(filepath.Dir(thisFile), "..", "postgres", "migrations")
	if err := postgres.RunMigrations(url, migrations); err != nil {
		t.Fatalf("migrations: %v", err)
	}
	p, err := postgres.NewPool(ctx, url)
	if err != nil {
		t.Fatalf("pool: %v", err)
	}
	t.Cleanup(p.Close)
	return p
}

// createTopic creates a one-partition topic and waits for its leader, per
// the fleet recipe (CreateTopics returns before the broker is done).
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
	deadline := time.Now().Add(30 * time.Second)
	for time.Now().Before(deadline) {
		if parts, err := conn.ReadPartitions(topic); err == nil && len(parts) == 1 && parts[0].Leader.ID != 0 {
			return
		}
		time.Sleep(200 * time.Millisecond)
	}
	t.Fatalf("topic %s never got a leader", topic)
}

// flakyAfterSend delivers every message to the real sink and THEN reports
// failure for the first message of the first pass: the exact "published,
// but the row was not marked" crash window. The row stays unpublished, so
// the next pass must republish it -- with the SAME persisted CloudEvents id.
type flakyAfterSend struct {
	inner relay.Sink
	mu    sync.Mutex
	fired bool
}

func (f *flakyAfterSend) Send(ctx context.Context, msgs ...outbox.Message) error {
	if err := f.inner.Send(ctx, msgs...); err != nil {
		return err
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if !f.fired {
		f.fired = true
		return errors.New("simulated crash after the broker ack, before the row was marked published")
	}
	return nil
}

func TestRelay_RealPostgresAndKafka_PublishesTheFourCloudEventsAndRetriesWithTheSameID(t *testing.T) {
	ctx := context.Background()
	brokers := startKafka(t)
	pool := startPostgresPool(t)

	topic := fmt.Sprintf("warehouse.warehouse-planning.events.itest-%d", time.Now().UnixNano())
	createTopic(t, brokers[0], topic)

	// --- create + publish a plan with a shortage against real Postgres ---
	start := time.Date(2026, 10, 5, 8, 0, 0, 0, time.UTC)
	end := time.Date(2026, 10, 5, 16, 0, 0, 0, time.UTC)
	pcs, paths := postgres.NewProcessCapacityRepo(pool), memory.NewProcessPathRepo()
	plans, ob, uow := postgres.NewCapacityPlanRepo(pool), postgres.NewOutboxRepo(pool), postgres.NewUnitOfWork(pool)
	enc := &outboundkafka.Encoder{NewID: uuid.NewString, Topic: topic}

	register := &usecases.RegisterProcessCapacityConstraint{Repo: pcs}
	for _, reg := range []struct {
		process processcapacity.ProcessType
		qty     float64
		unit    processcapacity.CapacityUnit
	}{{"PICK", 4000, processcapacity.UnitUnit}, {"REBIN", 2500, processcapacity.UnitUnit}, {"PACK", 1800, processcapacity.UnitPackage}} {
		if _, err := register.Handle(ctx, usecases.RegisterProcessCapacityConstraintCommand{
			ProcessType: reg.process, Location: "PATH-ZONE-A", WindowStart: start, WindowEnd: end,
			ConstraintType: processcapacity.ConstraintLabor, Quantity: reg.qty, Unit: reg.unit, Period: time.Hour,
		}); err != nil {
			t.Fatalf("register %s: %v", reg.process, err)
		}
	}
	if _, err := (&usecases.RegisterProcessPath{Repo: paths}).Handle(ctx, usecases.RegisterProcessPathCommand{
		ID: "pick-rebin-pack", Name: "Pick-Rebin-Pack", Steps: []processpath.ProcessType{"PICK", "REBIN", "PACK"},
	}); err != nil {
		t.Fatal(err)
	}

	upo, ppo := 2.5, 1.0
	create := &usecases.CreateCapacityPlan{
		PathCapacity: &usecases.GetProcessPathCapacity{ProcessPaths: paths, ProcessCapacities: pcs},
		Plans:        plans, Outbox: ob, Encoder: enc, UnitOfWork: uow,
	}
	publish := &usecases.PublishCapacityPlan{Plans: plans, Outbox: ob, Encoder: enc, UnitOfWork: uow}
	plan, err := create.Handle(ctx, usecases.CreateCapacityPlanCommand{
		WarehouseID: "WH-1", Location: "PATH-ZONE-A", WindowStart: start, WindowEnd: end,
		ProcessPathID: "pick-rebin-pack", AssignedDemand: 12000, UnitsPerOrder: &upo, PackagesPerOrder: &ppo,
	})
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	if _, err := publish.Handle(ctx, plan.ID()); err != nil {
		t.Fatalf("publish: %v", err)
	}

	// The ids persisted in the outbox, in row order.
	rows, err := pool.Query(ctx, `SELECT event_id, event_type FROM outbox_events ORDER BY id`)
	if err != nil {
		t.Fatal(err)
	}
	var wantIDs, wantTypes []string
	for rows.Next() {
		var id, typ string
		if err := rows.Scan(&id, &typ); err != nil {
			t.Fatal(err)
		}
		wantIDs, wantTypes = append(wantIDs, id), append(wantTypes, typ)
	}
	rows.Close()
	prefix := "com.warehouse.wes.warehouse-planning.capacityplan."
	if len(wantIDs) != 4 || wantTypes[0] != prefix+"CapacityPlanCreated" || wantTypes[1] != prefix+"CapacityPlanPublished" ||
		wantTypes[2] != prefix+"CapacityShortageDetected" || wantTypes[3] != prefix+"BottleneckDetected" {
		t.Fatalf("outbox rows = %v", wantTypes)
	}

	// --- run the relay against the real broker ---
	sink := outboundkafka.NewRelaySink(brokers)
	t.Cleanup(func() { _ = sink.Close() })
	flaky := &flakyAfterSend{inner: sink}
	r := relay.NewRelay(ob, flaky, slog.New(slog.NewTextHandler(io.Discard, nil)), relay.WithInterval(100*time.Millisecond))
	runCtx, stop := context.WithCancel(ctx)
	done := make(chan struct{})
	go func() { defer close(done); _ = r.Run(runCtx) }()
	t.Cleanup(func() { stop(); <-done })

	// --- consume what arrived ---
	reader := kafkago.NewReader(kafkago.ReaderConfig{Brokers: brokers, Topic: topic, Partition: 0, MinBytes: 1, MaxBytes: 10e6})
	t.Cleanup(func() { _ = reader.Close() })
	reader.SetOffset(kafkago.FirstOffset)

	// 4 events + 1 republished duplicate of the first row.
	type received struct {
		id, typ, subject, schema, contentType string
		key                                   string
		value                                 []byte
	}
	var got []received
	readCtx, cancel := context.WithTimeout(ctx, 60*time.Second)
	defer cancel()
	for len(got) < 5 {
		m, err := reader.ReadMessage(readCtx)
		if err != nil {
			t.Fatalf("read after %d messages: %v", len(got), err)
		}
		ev, err := cloudevents.Decode(m.Value)
		if err != nil {
			t.Fatalf("message %d is not a CloudEvent: %v", len(got), err)
		}
		rc := received{id: ev.ID(), typ: ev.Type(), subject: ev.Subject(), schema: ev.DataSchema(), key: string(m.Key), value: m.Value}
		for _, h := range m.Headers {
			if h.Key == "content-type" {
				rc.contentType = string(h.Value)
			}
		}
		got = append(got, rc)
	}

	// Ordering: same key -> one partition, the rows' order; the first row is
	// republished right after its simulated lost ack.
	wantOrder := []int{0, 0, 1, 2, 3}
	for i, rc := range got {
		idx := wantOrder[i]
		if rc.id != wantIDs[idx] || rc.typ != wantTypes[idx] {
			t.Errorf("message %d = %s %s, want %s %s", i, rc.id, rc.typ, wantIDs[idx], wantTypes[idx])
		}
		if rc.subject != plan.ID() || rc.key != plan.ID() {
			t.Errorf("message %d subject/key = %q/%q, want the plan id %q", i, rc.subject, rc.key, plan.ID())
		}
		if want := "urn:warehouse:warehouse-planning:events:" + wantTypes[idx][len(prefix):] + ":v1"; rc.schema != want {
			t.Errorf("message %d dataschema = %q, want %q", i, rc.schema, want)
		}
		if rc.contentType != "application/cloudevents+json; charset=UTF-8" {
			t.Errorf("message %d content-type = %q", i, rc.contentType)
		}
	}
	// THE retry guarantee: the republished message is byte-identical, same id.
	if got[0].id != got[1].id || string(got[0].value) != string(got[1].value) {
		t.Errorf("relay retry changed the message: %s vs %s", got[0].value, got[1].value)
	}

	// Eventually every row is marked published (and nothing more is sent).
	deadline := time.Now().Add(30 * time.Second)
	for {
		var unpublished int
		if err := pool.QueryRow(ctx, `SELECT count(*) FROM outbox_events WHERE published_at IS NULL`).Scan(&unpublished); err != nil {
			t.Fatal(err)
		}
		if unpublished == 0 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("%d outbox rows never got published", unpublished)
		}
		time.Sleep(100 * time.Millisecond)
	}
	var attempts int
	if err := pool.QueryRow(ctx, `SELECT attempts FROM outbox_events WHERE event_id = $1`, wantIDs[0]).Scan(&attempts); err != nil || attempts != 2 {
		t.Errorf("first row attempts = %d (%v), want 2 (failed once, then republished)", attempts, err)
	}
}

// With no pre-created topic the writer's AllowAutoTopicCreation makes the
// broker create it on the first send; kafka-go retries the not-yet-ready
// leader inside WriteMessages, so the relay delivers without any manual
// topic setup.
func TestRelay_RealKafka_AutoCreatesTheTopicOnFirstSend(t *testing.T) {
	ctx := context.Background()
	brokers := startKafka(t)
	topic := fmt.Sprintf("warehouse.warehouse-planning.events.itest-auto-%d", time.Now().UnixNano())

	store := memory.NewOutboxRepo()
	if err := store.Insert(ctx, outbox.Message{
		EventID: uuid.NewString(), Topic: topic, EventType: "com.warehouse.wes.warehouse-planning.capacityplan.BottleneckDetected",
		Subject: "plan-auto", Key: []byte("plan-auto"), Value: []byte(`{"specversion":"1.0","id":"x","source":"/s","type":"t","subject":"plan-auto","datacontenttype":"application/json","data":{}}`),
		Headers: []outbox.Header{{Key: "content-type", Value: "application/cloudevents+json; charset=UTF-8"}},
	}); err != nil {
		t.Fatal(err)
	}

	sink := outboundkafka.NewRelaySink(brokers)
	t.Cleanup(func() { _ = sink.Close() })
	r := relay.NewRelay(store, sink, slog.New(slog.NewTextHandler(io.Discard, nil)), relay.WithInterval(100*time.Millisecond))
	runCtx, stop := context.WithCancel(ctx)
	done := make(chan struct{})
	go func() { defer close(done); _ = r.Run(runCtx) }()
	t.Cleanup(func() { stop(); <-done })

	reader := kafkago.NewReader(kafkago.ReaderConfig{Brokers: brokers, Topic: topic, Partition: 0, MinBytes: 1, MaxBytes: 10e6})
	t.Cleanup(func() { _ = reader.Close() })
	reader.SetOffset(kafkago.FirstOffset)
	readCtx, cancel := context.WithTimeout(ctx, 60*time.Second)
	defer cancel()
	m, err := reader.ReadMessage(readCtx)
	if err != nil {
		t.Fatalf("nothing arrived on the auto-created topic: %v", err)
	}
	if string(m.Key) != "plan-auto" {
		t.Errorf("key = %q", m.Key)
	}
	deadline := time.Now().Add(30 * time.Second)
	for store.Unpublished() != 0 {
		if time.Now().After(deadline) {
			t.Fatal("row never marked published")
		}
		time.Sleep(50 * time.Millisecond)
	}
}
