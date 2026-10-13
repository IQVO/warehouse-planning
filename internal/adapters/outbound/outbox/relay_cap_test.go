package outbox_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"sync"
	"testing"

	"github.com/claudioed/warehouse-planning/internal/adapters/outbound/memory"
	relay "github.com/claudioed/warehouse-planning/internal/adapters/outbound/outbox"
	"github.com/claudioed/warehouse-planning/internal/application/outbox"
)

// poisonSink fails every message whose EventType is failOn and records the
// others.
type poisonSink struct {
	mu     sync.Mutex
	failOn string
	sent   []string
}

func (s *poisonSink) Send(_ context.Context, msgs ...outbox.Message) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, m := range msgs {
		if s.failOn != "" && m.EventType == s.failOn {
			return errors.New("message too large")
		}
		s.sent = append(s.sent, m.EventType)
	}
	return nil
}

// capRecords decodes JSON log lines and returns the ERROR ones carrying the
// attempt-cap message.
func capRecords(t *testing.T, buf *bytes.Buffer) []map[string]any {
	t.Helper()
	var out []map[string]any
	dec := json.NewDecoder(bytes.NewReader(buf.Bytes()))
	for dec.More() {
		var rec map[string]any
		if err := dec.Decode(&rec); err != nil {
			t.Fatalf("decode log line: %v", err)
		}
		if rec["level"] == "ERROR" && rec["msg"] == relay.AttemptCapReachedMsg {
			out = append(out, rec)
		}
	}
	return out
}

func seedTyped(t *testing.T, store *memory.OutboxRepo, eventType, key string) {
	t.Helper()
	if err := store.Insert(context.Background(), outbox.Message{EventID: eventType + "-" + key, Topic: "t", EventType: eventType, Key: []byte(key)}); err != nil {
		t.Fatal(err)
	}
}

// failPassesUntilCap runs maxAttempts failing passes and checks the
// attempt-cap ERROR appears exactly on the last one.
func failPassesUntilCap(t *testing.T, r *relay.Relay, logs *bytes.Buffer, maxAttempts int) {
	t.Helper()
	for i := 1; i <= maxAttempts; i++ {
		if n, err := r.RelayOnce(context.Background()); err == nil || n != 0 {
			t.Fatalf("pass %d: want the poison row to fail, got n=%d err=%v", i, n, err)
		}
		want := 0
		if i == maxAttempts {
			want = 1
		}
		if got := len(capRecords(t, logs)); got != want {
			t.Fatalf("pass %d: %d attempt-cap ERROR records, want %d", i, got, want)
		}
	}
}

// assertCapRecordFields checks the attempt-cap ERROR record names the row.
func assertCapRecordFields(t *testing.T, rec map[string]any) {
	t.Helper()
	for field, want := range map[string]any{"topic": "t", "event_type": "Poison", "key": "PLAN-CAP-1", "attempts": float64(3), "max_attempts": float64(3)} {
		if rec[field] != want {
			t.Errorf("%s = %v, want %v", field, rec[field], want)
		}
	}
	if _, ok := rec["outbox_id"]; !ok {
		t.Error("the ERROR record has no outbox_id")
	}
}

// Gap fix D12: a poison row blocks the relay only until it has failed
// maxAttempts times; the failure that reaches the cap is logged at ERROR
// with id, topic, type and key, and the good row behind it (same key)
// then publishes.
func TestRelay_MaxAttempts_PoisonRowExcludedThenGoodRowPublishes(t *testing.T) {
	store, sink := memory.NewOutboxRepo(), &poisonSink{failOn: "Poison"}
	seedTyped(t, store, "Poison", "PLAN-CAP-1")
	seedTyped(t, store, "Good", "PLAN-CAP-1")
	var logs bytes.Buffer
	r := relay.NewRelay(store, sink, slog.New(slog.NewJSONHandler(&logs, nil)), relay.WithMaxAttempts(3))

	failPassesUntilCap(t, r, &logs, 3)
	rec := capRecords(t, &logs)[0]
	assertCapRecordFields(t, rec)

	if n, err := r.RelayOnce(context.Background()); err != nil || n != 1 {
		t.Fatalf("pass after the cap: n=%d err=%v, want the good row published", n, err)
	}
	if len(sink.sent) != 1 || sink.sent[0] != "Good" {
		t.Fatalf("sent = %v, want only Good", sink.sent)
	}
	if store.Unpublished() != 1 || store.LastError(0) == "" {
		t.Fatal("the excluded row must stay unpublished with its last error")
	}
	if n, err := r.RelayOnce(context.Background()); err != nil || n != 0 {
		t.Fatalf("the excluded row was claimed again: n=%d err=%v", n, err)
	}
}

// WithMaxAttempts(0) (OUTBOX_MAX_ATTEMPTS=0) means unlimited: the poison row
// is retried forever and the row behind it never publishes.
func TestRelay_NoCap_PoisonRowBlocksLaterRows(t *testing.T) {
	store, sink := memory.NewOutboxRepo(), &poisonSink{failOn: "Poison"}
	seedTyped(t, store, "Poison", "PLAN-CAP-2")
	seedTyped(t, store, "Good", "PLAN-CAP-2")
	r := relay.NewRelay(store, sink, quiet(), relay.WithMaxAttempts(0))
	for i := 0; i < 25; i++ {
		if _, err := r.RelayOnce(context.Background()); err == nil {
			t.Fatalf("pass %d: the poison row must keep failing", i)
		}
	}
	if len(sink.sent) != 0 || store.Unpublished() != 2 {
		t.Fatalf("with no cap the good row must stay blocked: sent=%v unpublished=%d", sink.sent, store.Unpublished())
	}
}

// A negative cap is ignored (WithMaxAttempts(-1) leaves the earlier cap of 1
// in force), and the fleet default stays 20.
func TestRelay_MaxAttemptsNegativeIgnoredAndDefaultIsTwenty(t *testing.T) {
	if relay.DefaultMaxAttempts != 20 {
		t.Fatalf("DefaultMaxAttempts = %d, want 20 (fleet convention)", relay.DefaultMaxAttempts)
	}
	store, sink := memory.NewOutboxRepo(), &poisonSink{failOn: "Poison"}
	seedTyped(t, store, "Poison", "k")
	r := relay.NewRelay(store, sink, quiet(), relay.WithMaxAttempts(1), relay.WithMaxAttempts(-1))
	if _, err := r.RelayOnce(context.Background()); err == nil {
		t.Fatal("first pass: want the poison row to fail")
	}
	if n, err := r.RelayOnce(context.Background()); err != nil || n != 0 {
		t.Fatalf("second pass: n=%d err=%v, want the capped row excluded (a no-op pass)", n, err)
	}
}

// A failure below the cap carries the row identity for the caller.
func TestRelay_SendFailureCarriesTheRow(t *testing.T) {
	store, sink := memory.NewOutboxRepo(), &poisonSink{failOn: "Poison"}
	seedTyped(t, store, "Poison", "PLAN-9")
	_, err := relay.NewRelay(store, sink, quiet(), relay.WithMaxAttempts(5)).RelayOnce(context.Background())
	var sf *outbox.SendFailure
	if !errors.As(err, &sf) || sf.Attempts != 1 || sf.Message.EventType != "Poison" || string(sf.Message.Key) != "PLAN-9" {
		t.Fatalf("err = %v (%+v), want a SendFailure for the poison row with attempts 1", err, sf)
	}
}
