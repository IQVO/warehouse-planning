//go:build integration

// Gap fix D27 (ADR 0014): the W3C trace context of the transaction that
// wrote an outbox row crosses the outbox to Kafka. Real Postgres and a real
// Kafka broker, both through testcontainers (never KAFKA_BROKERS or
// localhost).
package outbox_test

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	kafkago "github.com/segmentio/kafka-go"
	"github.com/testcontainers/testcontainers-go"
	tckafka "github.com/testcontainers/testcontainers-go/modules/kafka"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/trace"

	outboundkafka "github.com/claudioed/warehouse-planning/internal/adapters/outbound/kafka"
	"github.com/claudioed/warehouse-planning/internal/adapters/outbound/memory"
	relay "github.com/claudioed/warehouse-planning/internal/adapters/outbound/outbox"
	"github.com/claudioed/warehouse-planning/internal/adapters/outbound/postgres"
	"github.com/claudioed/warehouse-planning/internal/application/usecases"
	"github.com/claudioed/warehouse-planning/internal/domain/processcapacity"
	"github.com/claudioed/warehouse-planning/internal/domain/processpath"
)

// installW3CTracing installs a real SDK tracer provider and the W3C
// trace-context propagator globally (what telemetry.Setup does in
// production), restoring the previous globals when the test ends.
func installW3CTracing(t *testing.T) trace.Tracer {
	t.Helper()
	prevTP, prevProp := otel.GetTracerProvider(), otel.GetTextMapPropagator()
	tp := sdktrace.NewTracerProvider()
	otel.SetTracerProvider(tp)
	otel.SetTextMapPropagator(propagation.TraceContext{})
	t.Cleanup(func() {
		_ = tp.Shutdown(context.Background())
		otel.SetTracerProvider(prevTP)
		otel.SetTextMapPropagator(prevProp)
	})
	return tp.Tracer("outbox-trace-itest")
}

// startTraceKafka boots a disposable broker for this file's tests (its own
// testcontainer, like the fleet reference) and pre-creates topic so the
// relay's first write does not race topic auto-creation.
func startTraceKafka(t *testing.T, ctx context.Context, topic string) []string {
	t.Helper()
	container, err := tckafka.Run(ctx, "confluentinc/confluent-local:7.6.1",
		tckafka.WithClusterID("warehouse-planning-outbox-trace-itest"))
	if err != nil {
		t.Fatalf("start Kafka container: %v", err)
	}
	t.Cleanup(func() {
		if err := testcontainers.TerminateContainer(container); err != nil {
			t.Logf("terminate Kafka container: %v", err)
		}
	})
	brokers, err := container.Brokers(ctx)
	if err != nil {
		t.Fatalf("resolve Kafka brokers: %v", err)
	}
	createTopic(t, brokers[0], topic)
	return brokers
}

func readOne(t *testing.T, ctx context.Context, brokers []string, topic string) kafkago.Message {
	t.Helper()
	reader := kafkago.NewReader(kafkago.ReaderConfig{Brokers: brokers, Topic: topic, Partition: 0, MinBytes: 1, MaxBytes: 1 << 20})
	defer func() { _ = reader.Close() }()
	reader.SetOffset(kafkago.FirstOffset)
	msg, err := reader.ReadMessage(ctx)
	if err != nil {
		t.Fatalf("read %s: %v", topic, err)
	}
	return msg
}

func headerValue(headers []kafkago.Header, key string) (string, bool) {
	for _, h := range headers {
		if h.Key == key {
			return string(h.Value), true
		}
	}
	return "", false
}

// storedTraceparents returns the traceparent stored in outbox_events.headers
// of every row on topic, in id order ("" when a row has none).
func storedTraceparents(t *testing.T, ctx context.Context, pool *pgxpool.Pool, topic string) []string {
	t.Helper()
	rows, err := pool.Query(ctx, `SELECT headers FROM outbox_events WHERE topic = $1 ORDER BY id`, topic)
	if err != nil {
		t.Fatalf("read stored headers: %v", err)
	}
	defer rows.Close()
	var out []string
	for rows.Next() {
		var raw []byte
		if err := rows.Scan(&raw); err != nil {
			t.Fatalf("scan stored headers: %v", err)
		}
		var hs []map[string]string
		if err := json.Unmarshal(raw, &hs); err != nil {
			t.Fatalf("decode stored headers %s: %v", raw, err)
		}
		tp := ""
		for _, h := range hs {
			if h["key"] == "traceparent" {
				tp = h["value"]
			}
		}
		out = append(out, tp)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("read stored headers: %v", err)
	}
	return out
}

// createPlanInSpan runs the CreateCapacityPlan use case against real
// Postgres inside ctx (the request span) and returns the plan id. The plan
// fits its capacity, so exactly one CapacityPlanCreated row lands on topic.
func createPlanInSpan(t *testing.T, ctx context.Context, pool *pgxpool.Pool, topic string) string {
	t.Helper()
	start := time.Date(2026, 10, 5, 8, 0, 0, 0, time.UTC)
	end := start.Add(8 * time.Hour)
	pcs, paths := postgres.NewProcessCapacityRepo(pool), memory.NewProcessPathRepo()
	if _, err := (&usecases.RegisterProcessCapacityConstraint{Repo: pcs}).Handle(ctx, usecases.RegisterProcessCapacityConstraintCommand{
		ProcessType: "PACK", Location: "TRACE-ZONE", WindowStart: start, WindowEnd: end,
		ConstraintType: processcapacity.ConstraintLabor, Quantity: 1000, Unit: processcapacity.UnitOrder, Period: time.Hour,
	}); err != nil {
		t.Fatalf("register capacity: %v", err)
	}
	if _, err := (&usecases.RegisterProcessPath{Repo: paths}).Handle(ctx, usecases.RegisterProcessPathCommand{
		ID: "pack-only", Name: "Pack only", Steps: []processpath.ProcessType{"PACK"},
	}); err != nil {
		t.Fatalf("register path: %v", err)
	}
	create := &usecases.CreateCapacityPlan{
		PathCapacity: &usecases.GetProcessPathCapacity{ProcessPaths: paths, ProcessCapacities: pcs,
			StationStandards: postgres.NewStationStandardRepo(pool), Tally: postgres.NewStorageTallyRepo(pool)},
		Plans: postgres.NewCapacityPlanRepo(pool), Outbox: postgres.NewOutboxRepo(pool),
		Encoder: &outboundkafka.Encoder{NewID: uuid.NewString, Topic: topic}, UnitOfWork: postgres.NewUnitOfWork(pool),
	}
	plan, err := create.Handle(ctx, usecases.CreateCapacityPlanCommand{
		WarehouseID: "WH-1", SiteID: "SIM1", Location: "TRACE-ZONE", WindowStart: start, WindowEnd: end,
		ProcessPathID: "pack-only", AssignedDemand: 100,
	})
	if err != nil {
		t.Fatalf("create plan: %v", err)
	}
	return plan.ID()
}

// TestOutboxTrace_TraceparentCrossesTheOutboxToKafka: a use case runs inside
// a request span; the outbox row stores that span's traceparent in
// outbox_events.headers; the relay (a later pass, no span of its own, a
// background context) sends it as a Kafka header; and a consumer extracting
// it with the global propagator lands on the request's trace and span.
func TestOutboxTrace_TraceparentCrossesTheOutboxToKafka(t *testing.T) {
	tracer := installW3CTracing(t)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	topic := fmt.Sprintf("warehouse.warehouse-planning.events.trace-itest-%d", time.Now().UnixNano())
	brokers := startTraceKafka(t, ctx, topic)
	pool := startPostgresPool(t)

	reqCtx, reqSpan := tracer.Start(ctx, "POST /capacity-plans", trace.WithSpanKind(trace.SpanKindServer))
	planID := createPlanInSpan(t, reqCtx, pool, topic)
	reqSpan.End()
	want := reqSpan.SpanContext()
	wantTraceparent := fmt.Sprintf("00-%s-%s-01", want.TraceID(), want.SpanID())
	if got := storedTraceparents(t, ctx, pool, topic); len(got) != 1 || got[0] != wantTraceparent {
		t.Fatalf("outbox row traceparents = %q, want exactly the request span's %q", got, wantTraceparent)
	}

	sink := outboundkafka.NewRelaySink(brokers)
	t.Cleanup(func() { _ = sink.Close() })
	r := relay.NewRelay(postgres.NewOutboxRepo(pool), sink, slog.New(slog.NewTextHandler(io.Discard, nil)), relay.WithMaxAttempts(20))
	// The relay runs without the request's context: continuity must come
	// from the stored row, not from the caller.
	if n, err := r.RelayOnce(context.Background()); err != nil || n != 1 {
		t.Fatalf("relay: n=%d err=%v", n, err)
	}

	msg := readOne(t, ctx, brokers, topic)
	if string(msg.Key) != planID {
		t.Fatalf("key = %q, want the plan id %q", msg.Key, planID)
	}
	got, ok := headerValue(msg.Headers, "traceparent")
	if !ok || got != wantTraceparent {
		t.Fatalf("Kafka traceparent = %q (present=%v), want %q", got, ok, wantTraceparent)
	}
	if ct, _ := headerValue(msg.Headers, "content-type"); ct == "" {
		t.Fatalf("content-type header lost on the outbox path: %+v", msg.Headers)
	}
	consumerCtx := otel.GetTextMapPropagator().Extract(context.Background(), kafkaHeaderCarrier(msg.Headers))
	parent := trace.SpanContextFromContext(consumerCtx)
	if parent.TraceID() != want.TraceID() || parent.SpanID() != want.SpanID() || !parent.IsRemote() {
		t.Fatalf("consumer would parent on %s/%s (remote=%v), want the request span %s/%s",
			parent.TraceID(), parent.SpanID(), parent.IsRemote(), want.TraceID(), want.SpanID())
	}
}

// TestOutboxTrace_RowWithoutTraceHeadersRelaysUnchanged pins the other half
// of D27: a row written with no trace context (no active span, or a row that
// predates trace capture) relays with exactly its stored headers, key and
// value, and carries no traceparent a consumer could mis-parent onto.
func TestOutboxTrace_RowWithoutTraceHeadersRelaysUnchanged(t *testing.T) {
	installW3CTracing(t)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	topic := fmt.Sprintf("warehouse.warehouse-planning.itest-no-trace-%d", time.Now().UnixNano())
	brokers := startTraceKafka(t, ctx, topic)
	pool := startPostgresPool(t)

	if _, err := pool.Exec(ctx, `
		INSERT INTO outbox_events (event_id, topic, event_type, subject, key, dataschema, value)
		VALUES ('evt-untraced', $1, 'com.example.Untraced', 'PLAN-UNTRACED', 'PLAN-UNTRACED', 'urn:test', '{}')
	`, topic); err != nil {
		t.Fatalf("insert outbox row: %v", err)
	}

	sink := outboundkafka.NewRelaySink(brokers)
	t.Cleanup(func() { _ = sink.Close() })
	r := relay.NewRelay(postgres.NewOutboxRepo(pool), sink, slog.New(slog.NewTextHandler(io.Discard, nil)), relay.WithMaxAttempts(20))
	if n, err := r.RelayOnce(ctx); err != nil || n != 1 {
		t.Fatalf("relay: n=%d err=%v", n, err)
	}

	msg := readOne(t, ctx, brokers, topic)
	if string(msg.Key) != "PLAN-UNTRACED" || string(msg.Value) != `{}` {
		t.Fatalf("row relayed changed: key=%q value=%q", msg.Key, msg.Value)
	}
	if len(msg.Headers) != 0 {
		t.Fatalf("a row without stored headers must relay with none, got %+v", msg.Headers)
	}
}

// kafkaHeaderCarrier adapts kafka-go headers to the OTel text-map carrier.
type kafkaHeaderCarrier []kafkago.Header

func (c kafkaHeaderCarrier) Get(key string) string {
	v, _ := headerValue(c, key)
	return v
}

func (c kafkaHeaderCarrier) Set(string, string) {}

func (c kafkaHeaderCarrier) Keys() []string {
	keys := make([]string, 0, len(c))
	for _, h := range c {
		keys = append(keys, h.Key)
	}
	return keys
}
