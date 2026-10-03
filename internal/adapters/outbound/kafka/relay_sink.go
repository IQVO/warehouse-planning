package kafka

import (
	"context"
	"fmt"

	segmentio "github.com/segmentio/kafka-go"

	"github.com/claudioed/warehouse-planning/internal/application/outbox"
)

// Writer is the subset of *segmentio.Writer RelaySink depends on, so unit
// tests can substitute a fake instead of a broker.
type Writer interface {
	WriteMessages(ctx context.Context, msgs ...segmentio.Message) error
}

// RelaySink writes already-encoded outbox messages to whichever topic each
// names. Its Writer has NO fixed Topic (kafka-go requires exactly one of
// Writer.Topic / Message.Topic), so the topic rides on each message.
//
// The writer follows the fleet's sync-writer settings: RequireAll acks, a
// short BatchTimeout, the kafka-go Hash balancer on the message key (the
// plan id, so one plan's events stay ordered on one partition) and
// AllowAutoTopicCreation (kafka-go retries a not-yet-ready topic leader
// inside WriteMessages). Constructing it never dials: the TCP connection
// happens lazily inside the first Send, so a broker outage at boot cannot
// crash the process.
type RelaySink struct {
	writer Writer
}

// NewRelaySink constructs a RelaySink over brokers. It does not dial.
func NewRelaySink(brokers []string) *RelaySink {
	return NewRelaySinkWithWriter(&segmentio.Writer{
		Addr:                   segmentio.TCP(brokers...),
		Balancer:               &segmentio.Hash{},
		BatchTimeout:           syncWriterBatchTimeout,
		RequiredAcks:           syncWriterRequiredAcks,
		AllowAutoTopicCreation: true,
	})
}

// NewRelaySinkWithWriter constructs a RelaySink over an explicit Writer
// (a fake in tests). The writer must NOT have a Topic set.
func NewRelaySinkWithWriter(w Writer) *RelaySink { return &RelaySink{writer: w} }

// Send writes msgs in one WriteMessages call.
func (s *RelaySink) Send(ctx context.Context, msgs ...outbox.Message) error {
	if len(msgs) == 0 {
		return nil
	}
	out := make([]segmentio.Message, len(msgs))
	for i, m := range msgs {
		if m.Topic == "" {
			return fmt.Errorf("kafka relay sink: message %d (%s) has no topic", i, m.EventType)
		}
		headers := make([]segmentio.Header, len(m.Headers))
		for j, h := range m.Headers {
			headers[j] = segmentio.Header{Key: h.Key, Value: []byte(h.Value)}
		}
		out[i] = segmentio.Message{Topic: m.Topic, Key: m.Key, Value: m.Value, Headers: headers}
	}
	if err := s.writer.WriteMessages(ctx, out...); err != nil {
		return fmt.Errorf("kafka relay sink: write %d message(s): %w", len(out), err)
	}
	return nil
}

// Close releases the underlying Kafka writer.
func (s *RelaySink) Close() error {
	if w, ok := s.writer.(*segmentio.Writer); ok {
		return w.Close()
	}
	return nil
}
