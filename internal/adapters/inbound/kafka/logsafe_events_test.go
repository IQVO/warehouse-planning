package kafka

import (
	"context"
	"errors"
	"log/slog"
	"testing"
	"time"

	kafkago "github.com/segmentio/kafka-go"
)

// TestConsumeLoopLogsNoEventDerivedLineBreaks drives the dead-letter path
// with a handler error and a dead-letter publish error that both quote the
// event: the retry, give-up and publish-retry logs must carry no line break.
func TestConsumeLoopLogsNoEventDerivedLineBreaks(t *testing.T) {
	rec := newCRLFRecorder()
	publishes := 0
	l := &consumeLoop{
		logger: slog.New(rec), name: "logsafe", maxAttempts: 2,
		handle: func(context.Context, kafkago.Message) error {
			return errors.New("use case refused event" + crlfForged)
		},
		deadLetter: func(context.Context, kafkago.Message, error) error {
			publishes++
			if publishes == 1 {
				return errors.New("dead-letter write failed" + crlfForged)
			}
			return nil
		},
	}
	noSleep := func(context.Context, time.Duration) error { return nil }
	msg := kafkago.Message{Topic: "logsafe.topic", Key: []byte("key" + crlfForged), Value: []byte("value" + crlfForged)}
	if err := l.handleWithDeadLetter(context.Background(), RetryPolicy{}.withDefaults(), noSleep, msg); err != nil {
		t.Fatal(err)
	}
	rec.assertNoLineBreaks(t, 3)
}
