package kafka

import (
	"context"
	"errors"
	"testing"
	"time"

	segmentio "github.com/segmentio/kafka-go"

	"github.com/claudioed/warehouse-planning/internal/application/outbox"
)

type fakeWriter struct {
	got []segmentio.Message
	err error
}

func (f *fakeWriter) WriteMessages(_ context.Context, msgs ...segmentio.Message) error {
	f.got = append(f.got, msgs...)
	return f.err
}

func TestRelaySink_SendRoutesByMessageTopicWithKeyAndHeaders(t *testing.T) {
	w := &fakeWriter{}
	s := NewRelaySinkWithWriter(w)
	err := s.Send(context.Background(),
		outbox.Message{Topic: "t1", EventType: "T", Key: []byte("k1"), Value: []byte("v1"), Headers: []outbox.Header{{Key: "content-type", Value: wantCT}}},
		outbox.Message{Topic: "t2", EventType: "T", Key: []byte("k2"), Value: []byte("v2")},
	)
	if err != nil {
		t.Fatalf("Send: %v", err)
	}
	if len(w.got) != 2 {
		t.Fatalf("wrote %d messages, want 2", len(w.got))
	}
	m := w.got[0]
	if m.Topic != "t1" || string(m.Key) != "k1" || string(m.Value) != "v1" || len(m.Headers) != 1 || m.Headers[0].Key != "content-type" || string(m.Headers[0].Value) != wantCT {
		t.Errorf("message 0 = %+v", m)
	}
	if w.got[1].Topic != "t2" || len(w.got[1].Headers) != 0 {
		t.Errorf("message 1 = %+v", w.got[1])
	}
}

func TestRelaySink_SendEmptyIsANoop(t *testing.T) {
	w := &fakeWriter{}
	if err := NewRelaySinkWithWriter(w).Send(context.Background()); err != nil || len(w.got) != 0 {
		t.Fatalf("Send() = %v, wrote %d", err, len(w.got))
	}
}

func TestRelaySink_SendRejectsMissingTopic(t *testing.T) {
	w := &fakeWriter{}
	err := NewRelaySinkWithWriter(w).Send(context.Background(), outbox.Message{EventType: "T"})
	if err == nil || len(w.got) != 0 {
		t.Fatalf("err = %v, wrote %d; want an error and no write", err, len(w.got))
	}
}

func TestRelaySink_SendWrapsWriterError(t *testing.T) {
	boom := errors.New("broker down")
	err := NewRelaySinkWithWriter(&fakeWriter{err: boom}).Send(context.Background(), outbox.Message{Topic: "t", EventType: "T"})
	if !errors.Is(err, boom) {
		t.Fatalf("err = %v, want it to wrap boom", err)
	}
}

// The production writer must carry the fleet's sync-writer settings and
// must be topic-less (the topic rides on each message).
func TestNewRelaySink_WriterSettings(t *testing.T) {
	s := NewRelaySink([]string{"broker-a:9092", "broker-b:9092"})
	w, ok := s.writer.(*segmentio.Writer)
	if !ok {
		t.Fatalf("writer is %T, want *segmentio.Writer", s.writer)
	}
	if w.RequiredAcks != segmentio.RequireAll {
		t.Errorf("RequiredAcks = %v, want RequireAll", w.RequiredAcks)
	}
	if w.BatchTimeout != 10*time.Millisecond {
		t.Errorf("BatchTimeout = %v, want 10ms", w.BatchTimeout)
	}
	if _, ok := w.Balancer.(*segmentio.Hash); !ok {
		t.Errorf("Balancer = %T, want *kafka.Hash", w.Balancer)
	}
	if !w.AllowAutoTopicCreation {
		t.Error("AllowAutoTopicCreation = false, want true")
	}
	if w.Topic != "" {
		t.Errorf("Topic = %q, want topic-less", w.Topic)
	}
	if got := w.Addr.String(); got != "broker-a:9092,broker-b:9092" {
		t.Errorf("Addr = %q", got)
	}
	// Constructing and closing never dials.
	if err := s.Close(); err != nil {
		t.Errorf("Close: %v", err)
	}
}

func TestRelaySink_CloseOnAFakeWriterIsANoop(t *testing.T) {
	if err := NewRelaySinkWithWriter(&fakeWriter{}).Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
}
