//go:build integration

package main_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"reflect"
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

	inboundhttp "github.com/claudioed/warehouse-planning/internal/adapters/inbound/http"
	inboundkafka "github.com/claudioed/warehouse-planning/internal/adapters/inbound/kafka"
	"github.com/claudioed/warehouse-planning/internal/adapters/outbound/analyticsstore"
	outboundkafka "github.com/claudioed/warehouse-planning/internal/adapters/outbound/kafka"
	"github.com/claudioed/warehouse-planning/internal/adapters/outbound/memory"
	relay "github.com/claudioed/warehouse-planning/internal/adapters/outbound/outbox"
	"github.com/claudioed/warehouse-planning/internal/adapters/outbound/postgres"
	"github.com/claudioed/warehouse-planning/internal/application/outbox"
	"github.com/claudioed/warehouse-planning/internal/application/usecases"
	"github.com/claudioed/warehouse-planning/internal/domain/processcapacity"
	"github.com/claudioed/warehouse-planning/internal/domain/processpath"
)

// The whole analytics read side against real infrastructure, all started with
// testcontainers (never skip-gated, never a hardcoded broker): plans are
// created and published against the OLTP Postgres (both topics' rows in one
// transaction), the relay drains the outbox to a real Kafka, the projector's
// consumer projects the analytics topic into a SEPARATE analytical Postgres,
// and the reports endpoints answer from it with the expected aggregates --
// while legacy, unknown-type, poison and duplicate messages share the topic.

func startPostgres(t *testing.T, db, migrations string) (string, *pgxpool.Pool) {
	t.Helper()
	ctx := context.Background()
	c, err := tcpostgres.Run(ctx, "postgres:16-alpine",
		tcpostgres.WithDatabase(db), tcpostgres.WithUsername("itest"), tcpostgres.WithPassword("itest"),
		testcontainers.WithWaitStrategy(wait.ForLog("database system is ready to accept connections").WithOccurrence(2).WithStartupTimeout(60*time.Second)))
	if err != nil {
		t.Fatalf("start postgres %s: %v", db, err)
	}
	t.Cleanup(func() { _ = c.Terminate(context.Background()) })
	url, err := c.ConnectionString(ctx, "sslmode=disable")
	if err != nil {
		t.Fatal(err)
	}
	_, thisFile, _, _ := runtime.Caller(0)
	if err := postgres.RunMigrations(url, filepath.Join(filepath.Dir(thisFile), migrations)); err != nil {
		t.Fatalf("migrations for %s: %v", db, err)
	}
	pool, err := postgres.NewPool(ctx, url)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	return url, pool
}

func startKafkaBroker(t *testing.T) []string {
	t.Helper()
	ctx := context.Background()
	c, err := tckafka.Run(ctx, "confluentinc/confluent-local:7.6.1", tckafka.WithClusterID("warehouse-planning-analytics-e2e"))
	if err != nil {
		t.Fatalf("start kafka: %v", err)
	}
	t.Cleanup(func() { _ = testcontainers.TerminateContainer(c) })
	brokers, err := c.Brokers(ctx)
	if err != nil {
		t.Fatal(err)
	}
	return brokers
}

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

// lostAckOnce delivers to the real sink and THEN reports failure once: the
// "published, but the row was not marked" crash window, so the relay
// republishes the first row (integration AND analytics copy) with the SAME id.
type lostAckOnce struct {
	inner relay.Sink
	mu    sync.Mutex
	fired bool
}

func (f *lostAckOnce) Send(ctx context.Context, msgs ...outbox.Message) error {
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

func at(day, hour, min int) time.Time { return time.Date(2026, 10, day, hour, min, 0, 0, time.UTC) }

type e2eSite struct{ warehouse, location string }

var (
	siteA = e2eSite{"WH-1", "PATH-ZONE-A"}
	siteB = e2eSite{"WH-2", "PATH-ZONE-B"}
)

func TestAnalyticsReadSide_EndToEnd(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	brokers := startKafkaBroker(t)
	_, oltp := startPostgres(t, "warehouse_planning_test", "internal/adapters/outbound/postgres/migrations")
	analyticalURL, _ := startPostgres(t, "warehouse_planning_analytics", "analytics/migrations")

	nonce := time.Now().UnixNano()
	integrationTopic := fmt.Sprintf("warehouse.warehouse-planning.events.e2e-%d", nonce)
	analyticsTopic := fmt.Sprintf("warehouse.warehouse-planning.analytics.e2e-%d", nonce)
	createTopic(t, brokers[0], integrationTopic)
	createTopic(t, brokers[0], analyticsTopic)

	// ---- OLTP: plans created and published, both topics' rows in ONE transaction ----
	enc := &outboundkafka.FanoutEncoder{
		Integration: &outboundkafka.Encoder{Topic: integrationTopic},
		Analytics:   &outboundkafka.AnalyticsEncoder{Topic: analyticsTopic},
		NewID:       uuid.NewString,
	}
	pcs, paths := postgres.NewProcessCapacityRepo(oltp), memory.NewProcessPathRepo()
	plans, ob, uow := postgres.NewCapacityPlanRepo(oltp), postgres.NewOutboxRepo(oltp), postgres.NewUnitOfWork(oltp)
	register := &usecases.RegisterProcessCapacityConstraint{Repo: pcs}
	windowStart, windowEnd := at(8, 8, 0), at(8, 16, 0)
	for _, site := range []e2eSite{siteA, siteB} {
		for _, reg := range []struct {
			process processcapacity.ProcessType
			qty     float64
			unit    processcapacity.CapacityUnit
		}{{"PICK", 4000, processcapacity.UnitUnit}, {"REBIN", 2500, processcapacity.UnitUnit}, {"PACK", 1800, processcapacity.UnitPackage}} {
			if _, err := register.Handle(ctx, usecases.RegisterProcessCapacityConstraintCommand{
				ProcessType: reg.process, Location: site.location, WindowStart: windowStart, WindowEnd: windowEnd,
				ConstraintType: processcapacity.ConstraintLabor, Quantity: reg.qty, Unit: reg.unit, Period: time.Hour,
			}); err != nil {
				t.Fatal(err)
			}
		}
	}
	if _, err := (&usecases.RegisterProcessPath{Repo: paths}).Handle(ctx, usecases.RegisterProcessPathCommand{
		ID: "pick-rebin-pack", Name: "Pick-Rebin-Pack", Steps: []processpath.ProcessType{"PICK", "REBIN", "PACK"}}); err != nil {
		t.Fatal(err)
	}
	var clock time.Time
	now := func() time.Time { return clock }
	create := &usecases.CreateCapacityPlan{
		PathCapacity: &usecases.GetProcessPathCapacity{ProcessPaths: paths, ProcessCapacities: pcs, StationStandards: postgres.NewStationStandardRepo(oltp), Tally: postgres.NewStorageTallyRepo(oltp)},
		Plans:        plans, Outbox: ob, Encoder: enc, UnitOfWork: uow, Now: now,
	}
	publish := &usecases.PublishCapacityPlan{Plans: plans, Outbox: ob, Encoder: enc, UnitOfWork: uow, Now: now}
	upo, ppo := 2.5, 1.0
	plan := func(site e2eSite, demand float64, created time.Time, publishedAfter time.Duration) {
		t.Helper()
		clock = created
		p, err := create.Handle(ctx, usecases.CreateCapacityPlanCommand{
			WarehouseID: site.warehouse, Location: site.location, WindowStart: windowStart, WindowEnd: windowEnd,
			ProcessPathID: "pick-rebin-pack", AssignedDemand: demand, UnitsPerOrder: &upo, PackagesPerOrder: &ppo})
		if err != nil {
			t.Fatalf("create: %v", err)
		}
		if publishedAfter > 0 {
			clock = created.Add(publishedAfter)
			if _, err := publish.Handle(ctx, p.ID()); err != nil {
				t.Fatalf("publish: %v", err)
			}
		}
	}
	plan(siteA, 12000, at(5, 8, 0), 10*time.Minute) // shortage 4000, latency 600 s
	plan(siteA, 6000, at(5, 9, 0), 3*time.Minute)   // within capacity, latency 180 s
	plan(siteA, 9000, at(5, 10, 0), 0)              // created, never published
	plan(siteB, 20000, at(6, 1, 0), 20*time.Minute) // shortage 12000, latency 1200 s

	// A1 and B1 (shortage): Created, Published, ShortageDetected, BottleneckDetected = 4 each;
	// A2 (no shortage): Created, Published = 2; A3 (draft): Created = 1; total 11.
	if n := countRows(t, oltp, `SELECT count(*) FROM outbox_events WHERE topic = $1`, analyticsTopic); n != 11 {
		t.Fatalf("analytics outbox rows = %d, want 11", n)
	}

	// ---- the topic also carries messages the projector must survive ----
	producer := &kafkago.Writer{Addr: kafkago.TCP(brokers...), Topic: analyticsTopic, BatchTimeout: 10 * time.Millisecond}
	t.Cleanup(func() { _ = producer.Close() })
	legacy := []byte(`{"event_id":"e-1","event_type":"CapacityPlanPublished","occurred_at":"2026-10-05T08:00:00Z","payload":{"plan_id":"x"}}`)
	unknown := hand(t, "evt-unknown", "com.warehouse.wes.warehouse-planning.capacityplan.SomethingNew", "plan-u", at(5, 11, 0), map[string]any{"plan_id": "plan-u"})
	poison := hand(t, "evt-poison", "com.warehouse.wes.warehouse-planning.capacityplan.CapacityPlanPublished", "plan-p", at(5, 11, 0), map[string]any{"plan_id": "plan-p", "location": "NOWHERE"})
	if err := producer.WriteMessages(ctx,
		kafkago.Message{Key: []byte("k"), Value: legacy}, kafkago.Message{Key: []byte("k"), Value: unknown}, kafkago.Message{Key: []byte("plan-p"), Value: poison}); err != nil {
		t.Fatal(err)
	}

	// ---- relay -> real Kafka (with one lost ack: a republish under the SAME ids) ----
	sink := outboundkafka.NewRelaySink(brokers)
	t.Cleanup(func() { _ = sink.Close() })
	r := relay.NewRelay(ob, &lostAckOnce{inner: sink}, slog.New(slog.NewTextHandler(io.Discard, nil)), relay.WithInterval(50*time.Millisecond))
	relayDone := make(chan struct{})
	go func() { defer close(relayDone); _ = r.Run(ctx) }()
	t.Cleanup(func() { cancel(); <-relayDone })

	// ---- the projector: consumer + writer pool over the analytical database ----
	writer, err := analyticsstore.NewPool(ctx, analyticalURL)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(writer.Close)
	readOnly, err := analyticsstore.NewReadOnlyPool(ctx, analyticalURL)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(readOnly.Close)
	consumer := inboundkafka.NewAnalyticsConsumer(brokers, analyticsTopic, fmt.Sprintf("planning-projector-e2e-%d", nonce),
		analyticsstore.NewProjection(writer), slog.New(slog.NewTextHandler(io.Discard, nil)))
	consumer.Retry = inboundkafka.RetryPolicy{Initial: 50 * time.Millisecond, Max: 200 * time.Millisecond}
	consumerDone := make(chan struct{})
	go func() { defer close(consumerDone); _ = consumer.Run(ctx) }()
	t.Cleanup(func() { cancel(); <-consumerDone; _ = consumer.Close() })

	// ---- the reports service ----
	srv := httptest.NewServer(inboundhttp.NewReportsRouter(&inboundhttp.ReportsServer{
		Reader: analyticsstore.NewReader(readOnly), Now: func() time.Time { return at(8, 12, 0) }}))
	t.Cleanup(srv.Close)
	query := "?from=2026-10-05T00:00:00Z&to=2026-10-07T00:00:00Z"

	type throughput struct {
		Days []struct {
			Day            string `json:"day"`
			WarehouseID    string `json:"warehouse_id"`
			Location       string `json:"location"`
			PlansCreated   int    `json:"plans_created"`
			PlansPublished int    `json:"plans_published"`
		} `json:"days"`
		Latency []struct {
			WarehouseID   string  `json:"warehouse_id"`
			Plans         int     `json:"plans"`
			MedianSeconds float64 `json:"median_seconds"`
			P95Seconds    float64 `json:"p95_seconds"`
		} `json:"latency"`
	}
	fetchThroughput := func() throughput {
		var out throughput
		getJSON(t, srv.URL+"/reports/plan-throughput"+query, &out)
		return out
	}
	waitFor(t, 90*time.Second, "all four plans projected", func() bool {
		th := fetchThroughput()
		created, published := 0, 0
		for _, d := range th.Days {
			created, published = created+d.PlansCreated, published+d.PlansPublished
		}
		return created == 4 && published == 3
	})

	assertAggregates := func(t *testing.T) {
		t.Helper()
		var bottleneck struct {
			Rows []map[string]any `json:"rows"`
		}
		getJSON(t, srv.URL+"/reports/bottleneck-frequency"+query, &bottleneck)
		wantRows := []map[string]any{
			{"warehouse_id": "WH-1", "location": "PATH-ZONE-A", "bottleneck_step": "REBIN", "binding_constraint": "LABOR", "plans": 2.0, "share": 1.0},
			{"warehouse_id": "WH-2", "location": "PATH-ZONE-B", "bottleneck_step": "REBIN", "binding_constraint": "LABOR", "plans": 1.0, "share": 1.0},
		}
		if !reflect.DeepEqual(bottleneck.Rows, wantRows) {
			t.Errorf("bottleneck-frequency = %v, want %v", bottleneck.Rows, wantRows)
		}

		var shortage struct {
			Days []map[string]any `json:"days"`
		}
		getJSON(t, srv.URL+"/reports/shortage-trend"+query, &shortage)
		wantDays := []map[string]any{
			{"day": "2026-10-05", "warehouse_id": "WH-1", "location": "PATH-ZONE-A", "plans_published": 2.0, "plans_with_shortage": 1.0, "total_shortage": 4000.0, "shortage_rate": 0.5},
			{"day": "2026-10-06", "warehouse_id": "WH-2", "location": "PATH-ZONE-B", "plans_published": 1.0, "plans_with_shortage": 1.0, "total_shortage": 12000.0, "shortage_rate": 1.0},
		}
		if !reflect.DeepEqual(shortage.Days, wantDays) {
			t.Errorf("shortage-trend = %v, want %v", shortage.Days, wantDays)
		}

		th := fetchThroughput()
		createdA, publishedA := 0, 0
		for _, d := range th.Days {
			if d.WarehouseID == "WH-1" {
				createdA, publishedA = createdA+d.PlansCreated, publishedA+d.PlansPublished
			}
		}
		if createdA != 3 || publishedA != 2 || len(th.Latency) < 2 {
			t.Fatalf("throughput = %+v", th)
		}
		a, b := th.Latency[0], th.Latency[1]
		if a.WarehouseID != "WH-1" || a.Plans != 2 || a.MedianSeconds != 390 || a.P95Seconds < 578.99 || a.P95Seconds > 579.01 {
			t.Errorf("WH-1 latency = %+v, want 2 plans, median 390 s, p95 579 s", a)
		}
		if b.WarehouseID != "WH-2" || b.Plans != 1 || b.MedianSeconds != 1200 || b.P95Seconds != 1200 {
			t.Errorf("WH-2 latency = %+v, want 1 plan at 1200 s", b)
		}
	}
	assertAggregates(t)

	// ---- the poison message reached the DLQ, byte for byte; legacy/unknown did not ----
	dlq := kafkago.NewReader(kafkago.ReaderConfig{Brokers: brokers, Topic: analyticsTopic + inboundkafka.DLQSuffix, Partition: 0, MinBytes: 1, MaxBytes: 10e6})
	t.Cleanup(func() { _ = dlq.Close() })
	_ = dlq.SetOffset(kafkago.FirstOffset)
	readCtx, stop := context.WithTimeout(ctx, 60*time.Second)
	defer stop()
	m, err := dlq.ReadMessage(readCtx)
	if err != nil {
		t.Fatalf("nothing reached the DLQ: %v", err)
	}
	if !bytes.Equal(m.Value, poison) {
		t.Errorf("DLQ value differs from the poison message:\n%s", m.Value)
	}
	quiet, quietStop := context.WithTimeout(ctx, 3*time.Second)
	defer quietStop()
	if extra, err := dlq.ReadMessage(quiet); err == nil {
		t.Errorf("a second message reached the DLQ (legacy and unknown types must be skipped, not dead-lettered): %s", extra.Value)
	}

	// ---- a redelivered analytics message (same id) changes nothing; a later marker proves it was consumed ----
	var dup []byte
	if err := oltp.QueryRow(ctx, `SELECT value FROM outbox_events WHERE topic = $1 AND event_type LIKE '%CapacityPlanPublished' ORDER BY id LIMIT 1`, analyticsTopic).Scan(&dup); err != nil {
		t.Fatal(err)
	}
	marker := hand(t, "evt-marker", "com.warehouse.wes.warehouse-planning.capacityplan.CapacityPlanCreated", "plan-marker", at(5, 12, 0),
		map[string]any{"plan_id": "plan-marker", "warehouse_id": "WH-9", "location": "NOWHERE-9", "shortage": 0})
	if err := producer.WriteMessages(ctx, kafkago.Message{Key: []byte("a"), Value: dup}, kafkago.Message{Key: []byte("b"), Value: marker}); err != nil {
		t.Fatal(err)
	}
	waitFor(t, 60*time.Second, "the marker plan projected", func() bool {
		th := fetchThroughput()
		for _, d := range th.Days {
			if d.WarehouseID == "WH-9" {
				return true
			}
		}
		return false
	})
	th := fetchThroughput()
	publishedA := 0
	for _, d := range th.Days {
		if d.WarehouseID == "WH-1" {
			publishedA += d.PlansPublished
		}
	}
	if publishedA != 2 {
		t.Errorf("after a redelivered event WH-1 published = %d, want still 2 (idempotent on the CloudEvents id)", publishedA)
	}

	// ---- the offsets only moved after success: every outbox row is published ----
	waitFor(t, 30*time.Second, "outbox drained", func() bool {
		return countRows(t, oltp, `SELECT count(*) FROM outbox_events WHERE published_at IS NULL`) == 0
	})
}

func hand(t *testing.T, id, typ, subject string, at time.Time, data map[string]any) []byte {
	t.Helper()
	b, err := json.Marshal(map[string]any{
		"specversion": "1.0", "id": id, "source": "/warehouse/warehouse-planning", "type": typ, "subject": subject,
		"datacontenttype": "application/json", "time": at.Format(time.RFC3339), "data": data,
	})
	if err != nil {
		t.Fatal(err)
	}
	return b
}

func getJSON(t *testing.T, url string, into any) {
	t.Helper()
	resp, err := http.Get(url)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("GET %s = %d %s", url, resp.StatusCode, body)
	}
	if err := json.Unmarshal(body, into); err != nil {
		t.Fatalf("decode %s: %v", body, err)
	}
}

func waitFor(t *testing.T, timeout time.Duration, what string, cond func() bool) {
	t.Helper()
	for deadline := time.Now().Add(timeout); ; time.Sleep(200 * time.Millisecond) {
		if cond() {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for: %s", what)
		}
	}
}

func countRows(t *testing.T, pool *pgxpool.Pool, query string, args ...any) int {
	t.Helper()
	var n int
	if err := pool.QueryRow(context.Background(), query, args...).Scan(&n); err != nil {
		t.Fatal(err)
	}
	return n
}
