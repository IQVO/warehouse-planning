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
	"github.com/claudioed/warehouse-planning/internal/application/usecases"
	"github.com/claudioed/warehouse-planning/internal/domain/processcapacity"
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
// real LocationSlotRegistered message produced onto a real Kafka topic
// is consumed and lands as a LOCATION CapacityConstraint in a real,
// Postgres-backed ProcessCapacityRepository, and that a subsequent
// LocationSlotDecommissioned decrements it back down.
func TestStorageCapacityConsumer_Integration_RealKafkaAndPostgres(t *testing.T) {
	brokers := startKafkaBroker(t)
	topic := uniqueTopic("warehouse.facility.events")
	createTopic(t, brokers, topic)

	pcRepo, processedRepo, tallyRepo := startPostgresForKafkaTests(t)
	register := &usecases.RegisterProcessCapacityConstraint{Repo: pcRepo}

	c := &kafkaconsumer.StorageCapacityConsumer{
		Reader:          kafkago.NewReader(kafkago.ReaderConfig{Brokers: brokers, Topic: topic, GroupID: uniqueGroupID("storage-capacity")}),
		Register:        register,
		Tally:           tallyRepo,
		ProcessedEvents: processedRepo,
		Logger:          testLogger(),
	}
	defer func() { _ = c.Close() }()
	runStorageConsumerInBackground(t, c)

	publishMessages(t, brokers, topic, kafkago.Message{
		Key: []byte("WH1-A-01"),
		Value: locationSlotWireEvent(t, fmt.Sprintf("itest-storage-reg-%d", time.Now().UnixNano()), "LocationSlotRegistered", time.Now().UTC(), map[string]any{
			"locationCode": "WH1-A-01",
			"zoneId":       "ZONE-A",
			"locationType": "BULK",
		}),
	})

	waitForCount := func(t *testing.T, processType, location string, want float64) {
		t.Helper()
		deadline := time.Now().Add(30 * time.Second)
		for time.Now().Before(deadline) {
			pc, err := pcRepo.FindByProcessLocationWindow(context.Background(),
				processcapacity.ProcessType(processType), location,
				kafkaconsumer.StandingWindowStart, kafkaconsumer.StandingWindowEnd)
			if err != nil {
				t.Fatalf("FindByProcessLocationWindow: %v", err)
			}
			if pc != nil {
				effective, _, err := pc.EffectiveRate()
				if err != nil {
					t.Fatalf("EffectiveRate: %v", err)
				}
				if effective.Quantity() == want {
					return
				}
			}
			time.Sleep(250 * time.Millisecond)
		}
		t.Fatalf("never observed %s/%s reach count %v", processType, location, want)
	}

	waitForCount(t, "STORAGE", "ZONE-A:BULK", 1)

	publishMessages(t, brokers, topic, kafkago.Message{
		Key: []byte("WH1-A-01"),
		Value: locationSlotWireEvent(t, fmt.Sprintf("itest-storage-decom-%d", time.Now().UnixNano()), "LocationSlotDecommissioned", time.Now().UTC(), map[string]any{
			"locationCode": "WH1-A-01",
		}),
	})

	waitForCount(t, "STORAGE", "ZONE-A:BULK", 0)
}
