package kafka

import (
	"context"

	kafkago "github.com/segmentio/kafka-go"
)

// Test-only access for the external kafka_test package: the integration
// test drives the PRODUCTION dead-letter writer and publish path against a
// real broker (a fake DeadLetterWriter can not catch a writer/message
// configuration that kafka-go rejects).

// NewDomainDLQWriterForTest returns the writer the domain consumers use.
func NewDomainDLQWriterForTest(brokers []string) DeadLetterWriter { return newDomainDLQWriter(brokers) }

// PublishDeadLetterForTest is publishDeadLetter.
func PublishDeadLetterForTest(ctx context.Context, w DeadLetterWriter, dlqTopic string, msg kafkago.Message, cause error) error {
	return publishDeadLetter(ctx, w, dlqTopic, msg, cause)
}

// IsPoisonForTest reports whether err is the deterministic-failure marker a
// consumer's HandleMessage returns for a message that must be dead-lettered
// at once (docs/adr/0017 decision 13).
func IsPoisonForTest(err error) bool { return isPoison(err) }
