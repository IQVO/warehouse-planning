//go:build integration

package kafka_test

// Real Kafka and real Postgres, both from testcontainers (the module is
// blank-imported so internal/architecture/fitness_test.go's
// TestKafkaIntegrationTestsUseTestcontainers scan sees it; the broker and
// database lifecycles live in main_integration_test.go and
// atomic_consumers_integration_test.go, shared across this package).

import (
	"context"
	"testing"
	"time"

	kafkago "github.com/segmentio/kafka-go"
	_ "github.com/testcontainers/testcontainers-go/modules/kafka"

	kafkaconsumer "github.com/claudioed/warehouse-planning/internal/adapters/inbound/kafka"
	"github.com/claudioed/warehouse-planning/internal/adapters/outbound/postgres"
	"github.com/claudioed/warehouse-planning/internal/application/usecases"
)

// REAL labor-performance standard events on a REAL Kafka topic land in the
// REAL Postgres copy (docs/adr/0022): a revision that arrives before the
// standard it supersedes, a site-scoped one, a redelivery of the same event
// id, an ignored type on the shared topic and an expected_seconds=0 message
// that is dead-lettered to the real .dlq topic.
func TestLaborStandardConsumer_Integration_RealKafkaAndPostgres(t *testing.T) {
	brokers := startKafkaBroker(t)
	topic := uniqueTopic("warehouse.labor-performance.events")
	createTopic(t, brokers, topic)
	createTopic(t, brokers, topic+kafkaconsumer.DLQSuffix)
	fx := startPgFixture(t)
	repo := postgres.NewLaborStandardRepo(fx.pool)

	c := &kafkaconsumer.LaborStandardConsumer{
		Reader:   kafkago.NewReader(kafkago.ReaderConfig{Brokers: brokers, Topic: topic, GroupID: uniqueGroupID("labor-standards")}),
		Record:   &usecases.RecordLaborStandard{UoW: fx.uow, ProcessedEvents: fx.processed, Standards: repo},
		Logger:   testLogger(),
		Retry:    itestRetry,
		DLQ:      kafkaconsumer.NewDomainDLQWriterForTest(brokers),
		DLQTopic: topic + kafkaconsumer.DLQSuffix,
	}
	t.Cleanup(func() { _ = c.Close() })
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	go func() { _ = c.Run(ctx) }()

	wire := func(id, typ string, at time.Time, data map[string]any) kafkago.Message {
		return kafkago.Message{Key: []byte("PICK"), Value: standardWire(t, id, typ, at, data)}
	}
	revision := definedData("std-2", 40)
	revision["effective_from"] = standardFrom.Add(time.Hour).Format(time.RFC3339)
	revision["previous_expected_seconds"] = 45
	site := definedData("std-3", 50)
	site["site_code"] = "SIM1"
	defined := wire("itest-def", typeStandardDefined, standardAt, definedData("std-1", 45))
	publishMessages(t, brokers, topic,
		wire("itest-rev", typeStandardRevised, standardAt.Add(time.Hour), revision),
		defined,
		defined, // redelivery: same event id
		wire("itest-perf", typeTaskPerformance, standardAt, map[string]any{"task_id": "task-1"}),
		wire("itest-zero", typeStandardDefined, standardAt, definedData("std-zero", 0)),
		wire("itest-site", typeStandardDefined, standardAt, site),
	)

	deadline := time.Now().Add(45 * time.Second)
	for {
		var n int
		if err := fx.pool.QueryRow(context.Background(), "SELECT count(*) FROM labor_standards").Scan(&n); err != nil {
			t.Fatal(err)
		}
		if n == 3 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("the copy holds %d standards, want 3", n)
		}
		time.Sleep(250 * time.Millisecond)
	}
	if dead := readDeadLetters(t, brokers, topic+kafkaconsumer.DLQSuffix, 1); len(dead) != 1 {
		t.Fatalf("dead letters = %d, want the expected_seconds=0 message", len(dead))
	}
	fleet, ok, err := repo.InForce(context.Background(), "SIM2", "PICK", standardWindow)
	if err != nil || !ok || fleet.ID() != "std-2" || fleet.ExpectedSeconds() != 40 {
		t.Fatalf("SIM2 in force = %+v %v %v, want the revision std-2", fleet, ok, err)
	}
	scoped, ok, err := repo.InForce(context.Background(), "SIM1", "PICK", standardWindow)
	if err != nil || !ok || scoped.ID() != "std-3" {
		t.Fatalf("SIM1 in force = %+v %v %v, want the site-scoped std-3", scoped, ok, err)
	}
	var claims int
	if err := fx.pool.QueryRow(context.Background(), "SELECT count(*) FROM processed_events WHERE consumer = 'labor-standard-consumer'").Scan(&claims); err != nil || claims != 3 {
		t.Fatalf("claims = %d, %v; want one per applied event id (3)", claims, err)
	}
}
