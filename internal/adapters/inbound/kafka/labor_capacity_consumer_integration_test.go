//go:build integration

package kafka_test

// This file is blank-imported with
// github.com/testcontainers/testcontainers-go/modules/kafka so it
// satisfies internal/architecture/fitness_test.go's
// TestKafkaIntegrationTestsUseTestcontainers scan (every Kafka-touching
// _integration_test.go file must reference that module) -- the actual
// broker lifecycle lives in main_integration_test.go, shared across this
// package's tests.

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	ce "github.com/cloudevents/sdk-go/v2/event"
	kafkago "github.com/segmentio/kafka-go"
	_ "github.com/testcontainers/testcontainers-go/modules/kafka"

	kafkaconsumer "github.com/claudioed/warehouse-planning/internal/adapters/inbound/kafka"
	"github.com/claudioed/warehouse-planning/internal/adapters/outbound/postgres"
	"github.com/claudioed/warehouse-planning/internal/application/usecases"
	"github.com/claudioed/warehouse-planning/internal/domain/processcapacity"
)

// createTopic creates topic explicitly (never relying on auto-creation)
// so a reader constructed immediately afterwards can always find its
// partitions.
func createTopic(t *testing.T, brokers []string, topic string) {
	t.Helper()
	conn, err := kafkago.Dial("tcp", brokers[0])
	if err != nil {
		t.Fatalf("dial %s: %v", brokers[0], err)
	}
	defer func() { _ = conn.Close() }()

	if err := conn.CreateTopics(kafkago.TopicConfig{
		Topic: topic, NumPartitions: 1, ReplicationFactor: 1,
	}); err != nil {
		t.Fatalf("create topic %s: %v", topic, err)
	}

	deadline := time.Now().Add(30 * time.Second)
	for time.Now().Before(deadline) {
		partitions, err := conn.ReadPartitions(topic)
		if err == nil && len(partitions) > 0 {
			return
		}
		time.Sleep(200 * time.Millisecond)
	}
	t.Fatalf("topic %s never became readable", topic)
}

func publishMessages(t *testing.T, brokers []string, topic string, msgs ...kafkago.Message) {
	t.Helper()
	w := &kafkago.Writer{
		Addr:     kafkago.TCP(brokers...),
		Topic:    topic,
		Balancer: &kafkago.LeastBytes{},
	}
	defer func() { _ = w.Close() }()

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	var err error
	for attempt := 0; attempt < 20; attempt++ {
		if err = w.WriteMessages(ctx, msgs...); err == nil {
			return
		}
		time.Sleep(500 * time.Millisecond)
	}
	t.Fatalf("publish to %s: %v", topic, err)
}

// shiftPlanCommittedWireEvent builds the exact CloudEvents 1.0
// structured-mode bytes workforce-management publishes, using the raw
// sdk-go event package to simulate the real upstream producer.
func shiftPlanCommittedWireEvent(t *testing.T, id string, occurredAt time.Time, data map[string]any) []byte {
	t.Helper()
	e := ce.New(ce.CloudEventsVersionV1)
	e.SetID(id)
	e.SetSource("/warehouse/workforce-management")
	e.SetType("com.warehouse.wes.workforce-management.shiftplan.ShiftPlanCommitted")
	e.SetSubject(fmt.Sprintf("%v/%v", data["building_id"], data["shift_id"]))
	e.SetTime(occurredAt)
	e.SetDataSchema("urn:warehouse:workforce-management:events:ShiftPlanCommitted:v1")
	if err := e.SetData("application/json", data); err != nil {
		t.Fatalf("SetData: %v", err)
	}
	b, err := json.Marshal(e)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	return b
}

// startPostgresForKafkaTests boots a real Postgres via testcontainers and
// runs this package's migrations, returning a ready ProcessCapacityRepo +
// ProcessedEventRepo pair backed by it (mirrors
// internal/adapters/outbound/postgres/process_capacity_repository_integration_test.go's
// recipe).
func startPostgresForKafkaTests(t *testing.T) (*postgres.ProcessCapacityRepo, *postgres.ProcessedEventRepo, *postgres.StorageTallyRepo) {
	t.Helper()
	databaseURL, pool := startPostgresPool(t)
	_ = databaseURL
	return postgres.NewProcessCapacityRepo(pool), postgres.NewProcessedEventRepo(pool), postgres.NewStorageTallyRepo(pool)
}

// TestLaborCapacityConsumer_Integration_RealKafkaAndPostgres proves a
// real ShiftPlanCommitted message produced onto a real Kafka topic is
// consumed and lands as a LABOR CapacityConstraint in a real,
// Postgres-backed ProcessCapacityRepository (Phase 1).
func TestLaborCapacityConsumer_Integration_RealKafkaAndPostgres(t *testing.T) {
	brokers := startKafkaBroker(t)
	topic := uniqueTopic("warehouse.workforce.events")
	createTopic(t, brokers, topic)

	pcRepo, processedRepo, _ := startPostgresForKafkaTests(t)
	register := &usecases.RegisterProcessCapacityConstraint{Repo: pcRepo}

	occurredAt := time.Date(2026, 10, 5, 6, 0, 0, 0, time.UTC)
	publishMessages(t, brokers, topic, kafkago.Message{
		Key: []byte("WH1/SHIFT-1"),
		Value: shiftPlanCommittedWireEvent(t, fmt.Sprintf("itest-labor-%d", time.Now().UnixNano()), occurredAt, map[string]any{
			"building_id":   "WH1",
			"shift_id":      "SHIFT-1",
			"path_id":       "pick",
			"planned_heads": 10,
			"planned_rate":  40.0,
			"planned_hours": 8.0,
		}),
	})

	c := &kafkaconsumer.LaborCapacityConsumer{
		Reader:          kafkago.NewReader(kafkago.ReaderConfig{Brokers: brokers, Topic: topic, GroupID: uniqueGroupID("labor-capacity")}),
		Register:        register,
		ProcessedEvents: processedRepo,
		Logger:          testLogger(),
	}
	defer func() { _ = c.Close() }()
	runLaborConsumerInBackground(t, c)

	windowEnd := occurredAt.Add(8 * time.Hour)
	deadline := time.Now().Add(30 * time.Second)
	for time.Now().Before(deadline) {
		pc, err := pcRepo.FindByProcessLocationWindow(context.Background(), "PICK", "WH1", occurredAt, windowEnd)
		if err != nil {
			t.Fatalf("FindByProcessLocationWindow: %v", err)
		}
		if pc != nil {
			effective, binding, err := pc.EffectiveRate()
			if err != nil {
				t.Fatalf("EffectiveRate: %v", err)
			}
			if binding != processcapacity.ConstraintLabor || effective.Quantity() != 400 {
				t.Fatalf("expected LABOR 400 UNIT/HOUR, got %v %v", effective.Quantity(), binding)
			}
			return
		}
		time.Sleep(250 * time.Millisecond)
	}
	t.Fatal("ShiftPlanCommitted published to a real Kafka topic was never applied to the Postgres-backed ProcessCapacityRepository")
}
