package kafka_test

import (
	"context"
	"testing"
	"time"
)

// End-to-end through the REAL StorageCapacityConsumer's Run with a DLQ
// wired: a message that NEVER succeeds is retried a bounded number of
// times (not forever), then dead-lettered exactly once with x-dlq-*
// headers, and the offset is committed so the loop moves on -- the fix
// for the "retries indefinitely" DLQ gap. LaborCapacityConsumer and
// OrderDemandConsumer share the identical wiring (same consumeLoop +
// deadletter.go), so this one consumer's Run-level proof stands in for
// all three per this package's existing test convention (see
// run_loop_test.go's Storage/Labor pairing).
func TestStorageConsumer_Run_TransientFailureExhaustsAttemptsThenDeadLetters(t *testing.T) {
	h := newStorageHarness()
	h.flaky.failNext(1 << 30) // never succeeds
	dlq := &fakeDLQWriter{}
	h.consumer.DLQ = dlq
	h.consumer.DLQTopic = "warehouse.facility.events.dlq"
	reader := newFakeReader(registeredEvent(t, "evt-1", "LOC-1", "Z", "BULK"))
	h.consumer.Reader = reader

	err := runUntilCommits(t, reader, 1, h.consumer.Run)
	if err != context.Canceled {
		t.Fatalf("Run = %v, want context.Canceled", err)
	}

	if got, want := reader.log(), []string{"fetch:0", "commit:0"}; len(got) != len(want) || got[0] != want[0] || got[1] != want[1] {
		t.Fatalf("reader events = %v, want exactly one fetch and one commit %v", got, want)
	}
	published := dlq.published()
	if len(published) != 1 {
		t.Fatalf("DLQ received %d messages, want exactly 1", len(published))
	}
	msg := published[0]
	if msg.Topic != "warehouse.facility.events.dlq" {
		t.Fatalf("DLQ message topic = %q", msg.Topic)
	}
	headerKeys := map[string]bool{}
	for _, hdr := range msg.Headers {
		headerKeys[hdr.Key] = true
	}
	for _, want := range []string{"x-dlq-source-topic", "x-dlq-error", "x-dlq-failed-at"} {
		if !headerKeys[want] {
			t.Errorf("DLQ message missing header %q (got %v)", want, msg.Headers)
		}
	}
	// A bounded number of attempts, not an unbounded hang: the test's own
	// 10s deadline (runUntilCommits) proves this already, but assert an
	// explicit, generous upper bound too so a future regression to
	// unbounded retry fails fast here instead of timing out.
	if calls := h.flaky.callCount(); calls == 0 || calls > 20 {
		t.Fatalf("tally mutation attempted %d times, want a small bounded number", calls)
	}
	if h.processed.Has(storageName, "evt-1") {
		t.Fatal("a dead-lettered message must not be marked processed (it never succeeded)")
	}
}

// Confirms the end-to-end timing budget: dead-lettering happens well
// within a few seconds, not after minutes of unbounded backoff.
func TestStorageConsumer_Run_DeadLettersPromptly(t *testing.T) {
	h := newStorageHarness()
	h.flaky.failNext(1 << 30)
	h.consumer.DLQ = &fakeDLQWriter{}
	h.consumer.DLQTopic = "x.dlq"
	reader := newFakeReader(registeredEvent(t, "evt-1", "LOC-1", "Z", "BULK"))
	h.consumer.Reader = reader

	start := time.Now()
	if err := runUntilCommits(t, reader, 1, h.consumer.Run); err != context.Canceled {
		t.Fatalf("Run = %v", err)
	}
	if elapsed := time.Since(start); elapsed > 5*time.Second {
		t.Fatalf("dead-lettering took %v, want well under the 10s test deadline", elapsed)
	}
}
