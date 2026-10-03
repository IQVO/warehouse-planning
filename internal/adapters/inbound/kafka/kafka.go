// Package kafka holds this service's INBOUND Kafka consumers (Phase 3):
// LaborCapacityConsumer (workforce-management's ShiftPlanCommitted) and
// StorageCapacityConsumer (facility-layout's
// LocationSlotRegistered/LocationSlotDecommissioned). Both decode
// CloudEvents 1.0 only via internal/adapters/kafka/cloudevents, dispatch
// on the FULL `type` string, and call the EXISTING
// usecases.RegisterProcessCapacityConstraint to upsert a ProcessCapacity
// constraint -- neither consumer duplicates that aggregate logic. See
// docs/adr/0001-warehouse-planning-bounded-context.md's Addendum and
// .claude/rules/integration-events.md for the confirmed upstream
// contracts.
package kafka

import (
	"context"
	"log/slog"

	kafkago "github.com/segmentio/kafka-go"
)

// Reader is the subset of *kafkago.Reader a consumer needs, so unit tests
// never need a live broker -- they call HandleMessage directly instead.
type Reader interface {
	ReadMessage(ctx context.Context) (kafkago.Message, error)
	Close() error
}

// readerConfig builds a kafka-go reader configuration shared by both
// consumers. GroupID always comes from the caller (an env var at the
// composition root -- never a literal here), satisfying
// internal/architecture/fitness_test.go's
// TestKafkaConsumerGroupNeverHardcodedInline.
//
// kafkago.NewReader itself does not dial the broker synchronously -- the
// real TCP connection is only established the first time ReadMessage
// runs inside the consumer's own background goroutine (see cmd/api's
// composition root), so constructing a Reader here never blocks process
// startup on Kafka being reachable.
func readerConfig(brokers []string, topic, groupID string) kafkago.ReaderConfig {
	return kafkago.ReaderConfig{
		Brokers: brokers,
		Topic:   topic,
		GroupID: groupID,
	}
}

func defaultLogger(logger *slog.Logger) *slog.Logger {
	if logger == nil {
		return slog.Default()
	}
	return logger
}
