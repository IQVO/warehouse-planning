//go:build integration

// Integration tests for the OUTBOUND Kafka publisher against a REAL broker
// via testcontainers (never a skip-gated external one, never a hardcoded
// localhost:9092 — see internal/architecture/fitness_test.go's
// TestKafkaIntegrationTestsUseTestcontainers): real domain events raised by
// a real CapacityPlan aggregate are encoded by the production Encoder into
// CloudEvents 1.0 outbox messages and written through the production
// RelaySink to a unique timestamp-suffixed topic; a kafka-go reader then
// consumes them back and the tests assert the mandatory envelope (type
// com.warehouse.wes.warehouse-planning.capacityplan.<Event>, specversion
// 1.0, id, source) and the payload, per the repo's .claude rules the
// envelope is mandatory.
package kafka_test

import (
	"context"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	kafkago "github.com/segmentio/kafka-go"
	"github.com/testcontainers/testcontainers-go"
	tckafka "github.com/testcontainers/testcontainers-go/modules/kafka"

	"github.com/claudioed/warehouse-planning/internal/adapters/kafka/cloudevents"
	outboundkafka "github.com/claudioed/warehouse-planning/internal/adapters/outbound/kafka"
	"github.com/claudioed/warehouse-planning/internal/domain/capacityplan"
	"github.com/claudioed/warehouse-planning/internal/domain/processcapacity"
)

// One broker for the whole package (containers are slow to boot);
// isolation comes from every test using its own unique topic.
var (
	pubBrokers   []string
	pubContainer testcontainers.Container
)

func TestMain(m *testing.M) {
	code := m.Run()
	if pubContainer != nil {
		if err := testcontainers.TerminateContainer(pubContainer); err != nil {
			fmt.Fprintf(os.Stderr, "terminate kafka container: %v\n", err)
		}
	}
	os.Exit(code)
}

// startPublisherBroker boots the shared confluent-local broker once.
func startPublisherBroker(t *testing.T) []string {
	t.Helper()
	if pubBrokers != nil {
		return pubBrokers
	}
	ctx := context.Background()
	container, err := tckafka.Run(ctx, "confluentinc/confluent-local:7.6.1",
		tckafka.WithClusterID("warehouse-planning-publisher-itest"))
	if err != nil {
		t.Fatalf("start kafka container: %v", err)
	}
	pubContainer = container
	if pubBrokers, err = container.Brokers(ctx); err != nil {
		t.Fatalf("resolve kafka brokers: %v", err)
	}
	return pubBrokers
}

// createPublisherTopic creates a one-partition topic and waits for its
// partition leader, per the fleet recipe (CreateTopics returns before the
// broker is done).
func createPublisherTopic(t *testing.T, broker, topic string) {
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

// planEvents raises the real domain events a shortage plan produces:
// Created at construction, Published + CapacityShortageDetected +
// BottleneckDetected at publish time.
func planEvents(t *testing.T) []capacityplan.Event {
	t.Helper()
	window, err := processcapacity.NewCapacityWindow(
		time.Date(2026, 10, 5, 8, 0, 0, 0, time.UTC),
		time.Date(2026, 10, 5, 16, 0, 0, 0, time.UTC))
	if err != nil {
		t.Fatalf("window: %v", err)
	}
	rate, err := processcapacity.NewCapacityRate(1000, processcapacity.UnitOrder, time.Hour)
	if err != nil {
		t.Fatalf("rate: %v", err)
	}
	plan, err := capacityplan.Create(capacityplan.CreateParams{
		ID: "plan-pub-it", WarehouseID: "WH-1", SiteID: "SIM1", Location: "PUB-LOC",
		Window: window, ProcessPathID: "pick-rebin-pack", AssignedDemand: 12000,
		PathRate: rate, BottleneckStep: "REBIN", BottleneckConstraint: processcapacity.ConstraintLabor,
	}, time.Date(2026, 10, 4, 17, 15, 30, 0, time.UTC))
	if err != nil {
		t.Fatalf("create plan: %v", err)
	}
	if err := plan.Publish(time.Date(2026, 10, 4, 17, 16, 0, 0, time.UTC)); err != nil {
		t.Fatalf("publish plan: %v", err)
	}
	events := append(plan.PullEvents(), plan.PullEvents()...)
	if len(events) != 4 {
		t.Fatalf("plan raised %d events, want 4", len(events))
	}
	return events
}

// TestPublisher_RealKafka_CloudEventsEnvelopeAndPayload publishes the four
// real events through the production Encoder + RelaySink on a unique topic,
// consumes them back, and asserts the mandatory CloudEvents 1.0 envelope
// and the payload of each.
func TestPublisher_RealKafka_CloudEventsEnvelopeAndPayload(t *testing.T) {
	ctx := context.Background()
	brokers := startPublisherBroker(t)
	topic := fmt.Sprintf("warehouse.warehouse-planning.events.pub-itest-%d", time.Now().UnixNano())
	createPublisherTopic(t, brokers[0], topic)

	events := planEvents(t)

	// The production publisher path: Encoder mints the CloudEvents ids and
	// builds the structured-mode messages; RelaySink is the exact writer
	// the relay in cmd/api uses against a real broker.
	encoder := &outboundkafka.Encoder{NewID: uuid.NewString, Topic: topic}
	msgs, err := encoder.Encode(events...)
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	sink := outboundkafka.NewRelaySink(brokers)
	t.Cleanup(func() { _ = sink.Close() })
	if err := sink.Send(ctx, msgs...); err != nil {
		t.Fatalf("send: %v", err)
	}

	// Consume back with a reader on the throwaway topic.
	reader := kafkago.NewReader(kafkago.ReaderConfig{Brokers: brokers, Topic: topic, Partition: 0, MinBytes: 1, MaxBytes: 10e6})
	t.Cleanup(func() { _ = reader.Close() })
	reader.SetOffset(kafkago.FirstOffset)

	type received struct {
		id, typ, source, subject, schema, contentType string
		key                                           string
		data                                          map[string]any
	}
	var got []received
	readCtx, cancel := context.WithTimeout(ctx, 60*time.Second)
	defer cancel()
	for len(got) < len(msgs) {
		m, err := reader.ReadMessage(readCtx)
		if err != nil {
			t.Fatalf("read after %d messages: %v", len(got), err)
		}
		ev, err := cloudevents.Decode(m.Value)
		if err != nil {
			t.Fatalf("message %d is not a valid CloudEvents 1.0 event: %v", len(got), err)
		}
		rc := received{id: ev.ID(), typ: ev.Type(), subject: ev.Subject(), key: string(m.Key)}
		rc.source = ev.Source()
		rc.schema = ev.DataSchema()
		var data map[string]any
		if err := ev.DataAs(&data); err != nil {
			t.Fatalf("message %d payload: %v", len(got), err)
		}
		rc.data = data
		for _, h := range m.Headers {
			if h.Key == "content-type" {
				rc.contentType = string(h.Value)
			}
		}
		got = append(got, rc)
	}

	// THE envelope contract, per event, in the order the rows were sent.
	const prefix = "com.warehouse.wes.warehouse-planning.capacityplan."
	wantTypes := []string{
		prefix + "CapacityPlanCreated",
		prefix + "CapacityPlanPublished",
		prefix + "CapacityShortageDetected",
		prefix + "BottleneckDetected",
	}
	if len(got) != 4 {
		t.Fatalf("consumed %d messages, want 4", len(got))
	}
	for i, rc := range got {
		if rc.typ != wantTypes[i] {
			t.Errorf("message %d type = %q, want %q", i, rc.typ, wantTypes[i])
		}
		if rc.id == "" || rc.id != string(msgs[i].EventID) {
			t.Errorf("message %d id = %q, want the outbox message's persisted id %q", i, rc.id, msgs[i].EventID)
		}
		if rc.source != cloudevents.Source {
			t.Errorf("message %d source = %q, want %q", i, rc.source, cloudevents.Source)
		}
		if rc.subject != "plan-pub-it" || rc.key != "plan-pub-it" {
			t.Errorf("message %d subject/key = %q/%q, want the plan id", i, rc.subject, rc.key)
		}
		wantSchema := "urn:warehouse:warehouse-planning:events:" + strings.TrimPrefix(wantTypes[i], prefix) + ":v1"
		if rc.schema != wantSchema {
			t.Errorf("message %d dataschema = %q, want %q", i, rc.schema, wantSchema)
		}
		if rc.contentType != cloudevents.MediaType {
			t.Errorf("message %d content-type = %q, want %q", i, rc.contentType, cloudevents.MediaType)
		}
	}

	// The payload: the published event carries the worked example's plan
	// figures (demand 12000, capacity over window 8000, shortage 4000,
	// bottleneck REBIN, site SIM1).
	pub := got[1].data
	for key, want := range map[string]any{
		"plan_id": "plan-pub-it", "warehouse_id": "WH-1", "site_id": "SIM1",
		"location": "PUB-LOC", "path_id": "pick-rebin-pack",
		"assigned_demand": 12000.0, "path_capacity": 1000.0,
		"capacity_over_window": 8000.0, "shortage": 4000.0, "bottleneck_step": "REBIN",
	} {
		if pub[key] != want {
			t.Errorf("CapacityPlanPublished payload %s = %v (%T), want %v", key, pub[key], pub[key], want)
		}
	}
	// The shortage event agrees on the shortage figure.
	if got[2].data["shortage"] != 4000.0 || got[2].data["bottleneck_step"] != "REBIN" {
		t.Errorf("CapacityShortageDetected payload = %v", got[2].data)
	}
	// The bottleneck event names the binding step and its rate.
	if got[3].data["bottleneck_step"] != "REBIN" || got[3].data["path_capacity"] != 1000.0 {
		t.Errorf("BottleneckDetected payload = %v", got[3].data)
	}
	// specversion 1.0 was already enforced by cloudevents.Decode (it
	// rejects anything else); the ids match the outbox rows exactly.
}

// TestPublisher_RealKafka_AnalyticsStreamCarriesItsOwnSchema proves the
// ADR 0005 fanout's analytics half: the same occurrence on the analytics
// stream keeps the SAME type and id but carries the analytics dataschema.
func TestPublisher_RealKafka_AnalyticsStreamCarriesItsOwnSchema(t *testing.T) {
	ctx := context.Background()
	brokers := startPublisherBroker(t)
	topic := fmt.Sprintf("warehouse.warehouse-planning.analytics.pub-itest-%d", time.Now().UnixNano())
	createPublisherTopic(t, brokers[0], topic)

	events := planEvents(t)
	encoder := &outboundkafka.AnalyticsEncoder{NewID: uuid.NewString, Topic: topic}
	msgs, err := encoder.Encode(events...)
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	sink := outboundkafka.NewRelaySink(brokers)
	t.Cleanup(func() { _ = sink.Close() })
	if err := sink.Send(ctx, msgs...); err != nil {
		t.Fatalf("send: %v", err)
	}

	reader := kafkago.NewReader(kafkago.ReaderConfig{Brokers: brokers, Topic: topic, Partition: 0, MinBytes: 1, MaxBytes: 10e6})
	t.Cleanup(func() { _ = reader.Close() })
	reader.SetOffset(kafkago.FirstOffset)

	readCtx, cancel := context.WithTimeout(ctx, 60*time.Second)
	defer cancel()
	m, err := reader.ReadMessage(readCtx)
	if err != nil {
		t.Fatalf("read first message: %v", err)
	}
	ev, err := cloudevents.Decode(m.Value)
	if err != nil {
		t.Fatalf("not a CloudEvent: %v", err)
	}
	if ev.Type() != "com.warehouse.wes.warehouse-planning.capacityplan.CapacityPlanCreated" {
		t.Errorf("type = %q", ev.Type())
	}
	if ev.DataSchema() != "urn:warehouse:warehouse-planning:analytics:CapacityPlanCreated:v1" {
		t.Errorf("dataschema = %q, want the analytics stream's", ev.DataSchema())
	}
}
