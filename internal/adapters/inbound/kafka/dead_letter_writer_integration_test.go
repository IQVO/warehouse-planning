//go:build integration

package kafka_test

import (
	"context"
	"errors"
	"testing"
	"time"

	kafkago "github.com/segmentio/kafka-go"
	_ "github.com/testcontainers/testcontainers-go/modules/kafka"

	kafkaconsumer "github.com/claudioed/warehouse-planning/internal/adapters/inbound/kafka"
)

// TestDomainDeadLetter_Integration_RealBrokerReceivesThePoisonMessage drives
// the production dead-letter writer and publish path of the three domain
// consumers (labor capacity, order demand, storage capacity) against a real
// broker: the poison message must land on "<topic>.dlq" with its key, value
// and x-dlq context. Before E6 the writer carried a fixed Topic while
// publishDeadLetter also set Message.Topic, which kafka-go refuses on every
// write, so no dead letter ever landed.
func TestDomainDeadLetter_Integration_RealBrokerReceivesThePoisonMessage(t *testing.T) {
	brokers := startKafkaBroker(t)
	for _, source := range []string{kafkaconsumer.LaborTopic, kafkaconsumer.OrderTopic, kafkaconsumer.FacilityTopic} {
		t.Run(source, func(t *testing.T) {
			sourceTopic := uniqueTopic(source)
			dlqTopic := sourceTopic + kafkaconsumer.DLQSuffix
			createTopic(t, brokers, dlqTopic)

			w := kafkaconsumer.NewDomainDLQWriterForTest(brokers)
			t.Cleanup(func() { _ = w.Close() })

			ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
			defer cancel()
			poison := kafkago.Message{Topic: sourceTopic, Partition: 0, Offset: 41, Key: []byte("k-1"), Value: []byte("{not json")}
			if err := kafkaconsumer.PublishDeadLetterForTest(ctx, w, dlqTopic, poison, errors.New("malformed payload")); err != nil {
				t.Fatalf("publish dead letter to %s: %v", dlqTopic, err)
			}

			r := kafkago.NewReader(kafkago.ReaderConfig{Brokers: brokers, Topic: dlqTopic, Partition: 0, MinBytes: 1, MaxBytes: 1 << 20})
			t.Cleanup(func() { _ = r.Close() })
			got, err := r.ReadMessage(ctx)
			if err != nil {
				t.Fatalf("read %s: %v", dlqTopic, err)
			}
			if string(got.Key) != "k-1" || string(got.Value) != "{not json" {
				t.Fatalf("dead letter = key %q value %q, want the original key and value", got.Key, got.Value)
			}
			headers := map[string]string{}
			for _, h := range got.Headers {
				headers[h.Key] = string(h.Value)
			}
			if headers["x-dlq-source-topic"] != sourceTopic || headers["x-dlq-source-offset"] != "41" || headers["x-dlq-error"] != "malformed payload" {
				t.Fatalf("x-dlq headers = %v", headers)
			}
		})
	}
}
