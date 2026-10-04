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
)

// locationSlotWireEvent builds the exact CloudEvents 1.0 structured-mode
// bytes facility-layout publishes, using the raw sdk-go event package to
// simulate the real upstream producer.
func locationSlotWireEvent(t *testing.T, id, eventName string, occurredAt time.Time, data map[string]any) []byte {
	t.Helper()
	e := ce.New(ce.CloudEventsVersionV1)
	e.SetID(id)
	e.SetSource("/warehouse/facility-layout")
	e.SetType("com.warehouse.wms.facility-layout.locationslot." + eventName)
	e.SetSubject(fmt.Sprintf("%v", data["locationCode"]))
	e.SetTime(occurredAt)
	e.SetDataSchema("urn:warehouse:facility-layout:events:" + eventName + ":v1")
	if err := e.SetData("application/json", data); err != nil {
		t.Fatalf("SetData: %v", err)
	}
	b, err := json.Marshal(e)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	return b
}

// TestStorageCapacityConsumer_Integration_RealKafkaAndPostgres proves a
// real LocationSlotRegistered message produced onto a real Kafka topic is
// consumed and lands in the Postgres tally -- and ONLY there: no
// ProcessCapacity row is written, the site-scoped read side sees the station,
// and a subsequent LocationSlotDecommissioned decrements it back down.
func TestStorageCapacityConsumer_Integration_RealKafkaAndPostgres(t *testing.T) {
	brokers := startKafkaBroker(t)
	topic := uniqueTopic("warehouse.facility.events")
	createTopic(t, brokers, topic)

	fx := startPgFixture(t)
	c := &kafkaconsumer.StorageCapacityConsumer{
		Reader:          kafkago.NewReader(kafkago.ReaderConfig{Brokers: brokers, Topic: topic, GroupID: uniqueGroupID("storage-capacity")}),
		Tally:           fx.tally,
		ProcessedEvents: fx.processed,
		UoW:             fx.uow,
		Logger:          testLogger(),
	}
	defer func() { _ = c.Close() }()
	runStorageConsumerInBackground(t, c)

	publishMessages(t, brokers, topic, kafkago.Message{
		Key: []byte("SIM1-OPS-WC-01"),
		Value: locationSlotWireEvent(t, fmt.Sprintf("itest-storage-reg-%d", time.Now().UnixNano()), "LocationSlotRegistered", time.Now().UTC(), map[string]any{
			"locationCode": "SIM1-OPS-WC-01",
			"zoneId":       "SIM1-OPS-WC",
			"role":         "WorkCenter",
			"activities":   []string{"Pack"},
		}),
	})

	waitForStations := func(t *testing.T, want int) {
		t.Helper()
		deadline := time.Now().Add(30 * time.Second)
		for time.Now().Before(deadline) {
			got, err := fx.tally.StationCount(context.Background(), "SIM1", "PACK")
			if err != nil {
				t.Fatalf("StationCount: %v", err)
			}
			if got == want {
				return
			}
			time.Sleep(250 * time.Millisecond)
		}
		t.Fatalf("never observed SIM1's PACK stations reach %d", want)
	}

	waitForStations(t, 1)
	if n := fx.tallyCount(t, "SIM1-OPS-WC", "STATION", "PACK"); n != 1 {
		t.Fatalf("tally row = %d, want 1", n)
	}

	publishMessages(t, brokers, topic, kafkago.Message{
		Key: []byte("SIM1-OPS-WC-01"),
		Value: locationSlotWireEvent(t, fmt.Sprintf("itest-storage-decom-%d", time.Now().UnixNano()), "LocationSlotDecommissioned", time.Now().UTC(), map[string]any{
			"locationCode": "SIM1-OPS-WC-01",
		}),
	})
	waitForStations(t, 0)

	if n := fx.processCapacityRows(t); n != 0 {
		t.Fatalf("process_capacity rows = %d, want 0: the facility consumer only maintains the tally", n)
	}
}
