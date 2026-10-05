// Package kafka's deadletter.go gives the three domain consumers
// (storage/facility, labor, order-demand) dead-letter handling, mirroring
// the fleet pattern (order-management's RepromiseConsumer,
// fulfillment-execution's Consumer): a TRANSIENT failure is retried a
// bounded number of times with capped backoff (consumeLoop's existing
// policy), and only once that bound is exhausted is the message
// published to "<topic>.dlq" with x-dlq-* headers, instead of blocking
// the partition forever (the bug this fixes -- see the 2026-10-04 ADR
// audit's warehouse-planning item on missing DLQ handling).
//
// A deterministic problem (not a CloudEvents 1.0 message, unknown type,
// malformed payload, missing fields, domain validation failure) is
// UNCHANGED: each consumer's HandleMessage still returns nil for those
// and the run loop commits past them without ever retrying or
// dead-lettering -- only a genuinely transient/infrastructure error
// (claim, find, save, begin/commit) reaches this bounded-retry-then-DLQ
// path, because only those return a non-nil error from HandleMessage.
//
// This is a DIFFERENT (and newer) policy than AnalyticsConsumer's own
// (analytics_consumer.go): that consumer deliberately never dead-letters
// a transient failure (ADR 0005 Decision §4), because losing analytics
// to a brief database outage is unacceptable there. The three domain
// consumers have no such ADR and, before this change, inherited the
// generic consumeLoop's unconditional infinite retry -- a real
// availability bug (a stuck poison-shaped transient failure wedges the
// partition forever) that this file fixes.
package kafka

import (
	"context"
	"strconv"
	"time"

	kafkago "github.com/segmentio/kafka-go"
)

// domainMaxHandlerAttempts bounds a domain consumer's in-loop retry of a
// transient failure before the message is dead-lettered: 1 initial
// attempt plus up to 4 retries. Chosen (rather than the fleet's smaller
// maxHandlerAttempts=3 used by order-management/fulfillment-execution,
// which retry inside ONE handler call) to stay comfortably above this
// package's existing "fails twice then succeeds" test fixtures while
// still bounding the previously-infinite retry.
const domainMaxHandlerAttempts = 5

// newDomainDLQWriter builds the dead-letter writer for one of the three
// domain Kafka consumers, publishing to topic+DLQSuffix. It follows the
// SAME sync-writer settings as this service's outbox relay
// (outbound/kafka.RelaySink): a Hash balancer on the message key (so one
// key's dead-lettered messages land on one partition, preserving
// relative order for that key), RequireAll acks, AllowAutoTopicCreation
// (the DLQ topic has never been written to before the first poison
// message) and a short BatchTimeout so a lone synchronous write is not
// held for kafka-go's 1s default.
func newDomainDLQWriter(brokers []string, topic string) DeadLetterWriter {
	return &kafkago.Writer{
		Addr:                   kafkago.TCP(brokers...),
		Topic:                  topic + DLQSuffix,
		Balancer:               &kafkago.Hash{},
		RequiredAcks:           kafkago.RequireAll,
		BatchTimeout:           10 * time.Millisecond,
		AllowAutoTopicCreation: true,
	}
}

// publishDeadLetter publishes msg (its original key/value/headers, plus
// x-dlq-source-topic/x-dlq-error/x-dlq-failed-at context) to dlqTopic via
// w, retrying while the auto-created topic has no leader yet (writeDLQ,
// shared with AnalyticsConsumer). The source topic/partition/offset are
// taken from msg itself, so callers never need to pass them separately.
func publishDeadLetter(ctx context.Context, w DeadLetterWriter, dlqTopic string, msg kafkago.Message, cause error) error {
	headers := append([]kafkago.Header{}, msg.Headers...)
	headers = append(headers,
		kafkago.Header{Key: "x-dlq-source-topic", Value: []byte(msg.Topic)},
		kafkago.Header{Key: "x-dlq-source-partition", Value: []byte(strconv.Itoa(msg.Partition))},
		kafkago.Header{Key: "x-dlq-source-offset", Value: []byte(strconv.FormatInt(msg.Offset, 10))},
		kafkago.Header{Key: "x-dlq-error", Value: []byte(cause.Error())},
		kafkago.Header{Key: "x-dlq-failed-at", Value: []byte(time.Now().UTC().Format(time.RFC3339))},
	)
	return writeDLQ(ctx, w, kafkago.Message{Topic: dlqTopic, Key: msg.Key, Value: msg.Value, Headers: headers})
}
