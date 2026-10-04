package outbox_test

import (
	"bytes"
	"context"
	"errors"
	"io"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/claudioed/warehouse-planning/internal/adapters/outbound/memory"
	relay "github.com/claudioed/warehouse-planning/internal/adapters/outbound/outbox"
	"github.com/claudioed/warehouse-planning/internal/application/outbox"
)

func quiet() *slog.Logger { return slog.New(slog.NewTextHandler(io.Discard, nil)) }

type recordingSink struct {
	mu   sync.Mutex
	ids  []string
	fail error
}

func (s *recordingSink) Send(_ context.Context, msgs ...outbox.Message) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.fail != nil {
		return s.fail
	}
	for _, m := range msgs {
		s.ids = append(s.ids, m.EventID)
	}
	return nil
}

func (s *recordingSink) sent() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]string(nil), s.ids...)
}

func (s *recordingSink) setFail(err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.fail = err
}

func seed(t *testing.T, store *memory.OutboxRepo, ids ...string) {
	t.Helper()
	for _, id := range ids {
		if err := store.Insert(context.Background(), outbox.Message{EventID: id, EventType: "T"}); err != nil {
			t.Fatal(err)
		}
	}
}

func TestRelayOnce_DrainsAndMarksPublished(t *testing.T) {
	store, sink := memory.NewOutboxRepo(), &recordingSink{}
	seed(t, store, "a", "b")
	r := relay.NewRelay(store, sink, quiet())
	n, err := r.RelayOnce(context.Background())
	if err != nil || n != 2 {
		t.Fatalf("RelayOnce = %d, %v", n, err)
	}
	if got := sink.sent(); len(got) != 2 || got[0] != "a" || got[1] != "b" {
		t.Errorf("sent = %v", got)
	}
	if store.Unpublished() != 0 {
		t.Errorf("Unpublished = %d, want 0", store.Unpublished())
	}
}

// A full batch must be followed IMMEDIATELY by another pass: with a 1h
// interval and batch size 2, 5 rows only drain promptly if the relay does
// not sleep between full batches.
func TestRun_FullBatchIsFollowedImmediatelyByAnotherPass(t *testing.T) {
	store, sink := memory.NewOutboxRepo(), &recordingSink{}
	seed(t, store, "1", "2", "3", "4", "5")
	r := relay.NewRelay(store, sink, quiet(), relay.WithInterval(time.Hour), relay.WithBatchSize(2))

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- r.Run(ctx) }()

	waitFor(t, func() bool { return store.Unpublished() == 0 })
	cancel()
	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Fatalf("Run err = %v, want context.Canceled", err)
	}
	if got := sink.sent(); len(got) != 5 {
		t.Errorf("sent %v, want all 5 in order", got)
	}
}

func TestRun_PicksUpNewRowsEveryInterval(t *testing.T) {
	store, sink := memory.NewOutboxRepo(), &recordingSink{}
	r := relay.NewRelay(store, sink, quiet(), relay.WithInterval(10*time.Millisecond))
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() { _ = r.Run(ctx) }()

	time.Sleep(30 * time.Millisecond) // empty passes
	seed(t, store, "late")
	waitFor(t, func() bool { return len(sink.sent()) == 1 })
}

// A failing broker leaves the row unpublished; once it recovers the SAME
// message (same persisted id) is republished.
func TestRun_RetriesAFailedPassAndRepublishesTheSameID(t *testing.T) {
	store, sink := memory.NewOutboxRepo(), &recordingSink{}
	sink.setFail(errors.New("broker down"))
	seed(t, store, "evt-1")
	r := relay.NewRelay(store, sink, quiet(), relay.WithInterval(10*time.Millisecond))
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() { _ = r.Run(ctx) }()

	waitFor(t, func() bool { return store.LastError(0) != "" })
	if store.Unpublished() != 1 {
		t.Fatal("row was marked published despite the send failure")
	}
	sink.setFail(nil)
	waitFor(t, func() bool { return store.Unpublished() == 0 })
	if got := sink.sent(); len(got) != 1 || got[0] != "evt-1" {
		t.Errorf("sent = %v, want [evt-1]", got)
	}
}

func TestRun_StopsPromptlyOnCancelAndNeverDials(t *testing.T) {
	r := relay.NewRelay(memory.NewOutboxRepo(), &recordingSink{}, quiet(), relay.WithInterval(time.Hour))
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- r.Run(ctx) }()
	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("Run err = %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Run ignored cancellation")
	}
}

func TestRun_ReturnsImmediatelyForAnAlreadyCancelledContext(t *testing.T) {
	store := memory.NewOutboxRepo()
	seed(t, store, "x")
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	err := relay.NewRelay(store, &recordingSink{}, quiet()).Run(ctx)
	if !errors.Is(err, context.Canceled) || store.Unpublished() != 1 {
		t.Fatalf("Run = %v, unpublished %d; want Canceled and an untouched outbox", err, store.Unpublished())
	}
}

func TestOptionsIgnoreNonPositiveValues(t *testing.T) {
	store, sink := memory.NewOutboxRepo(), &recordingSink{}
	seed(t, store, "a", "b", "c")
	// WithBatchSize(0)/(-1) must keep the default (100), so one pass drains all 3.
	r := relay.NewRelay(store, sink, nil, relay.WithBatchSize(0), relay.WithBatchSize(-1), relay.WithInterval(0), relay.WithInterval(-time.Second))
	if n, err := r.RelayOnce(context.Background()); err != nil || n != 3 {
		t.Fatalf("RelayOnce = %d, %v; want 3", n, err)
	}
}

func TestLogSink_LogsEachMessageAndSucceeds(t *testing.T) {
	var buf bytes.Buffer
	sink := relay.LogSink{Logger: slog.New(slog.NewTextHandler(&buf, nil))}
	err := sink.Send(context.Background(),
		outbox.Message{EventID: "id-1", Topic: "t", EventType: "com.x.A", Subject: "s1"},
		outbox.Message{EventID: "id-2", Topic: "t", EventType: "com.x.B", Subject: "s1"},
	)
	if err != nil {
		t.Fatalf("Send: %v", err)
	}
	out := buf.String()
	for _, want := range []string{"id-1", "id-2", "com.x.A", "com.x.B", "topic=t", "subject=s1"} {
		if !strings.Contains(out, want) {
			t.Errorf("log output missing %q:\n%s", want, out)
		}
	}
	// A zero-value LogSink falls back to the default logger instead of panicking.
	if err := (relay.LogSink{}).Send(context.Background(), outbox.Message{}); err != nil {
		t.Fatalf("zero LogSink: %v", err)
	}
}

func waitFor(t *testing.T, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatal("condition not met within 5s")
}
