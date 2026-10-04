package kafka_test

import (
	"context"
	"reflect"
	"testing"
	"time"

	kafkago "github.com/segmentio/kafka-go"
)

// runUntilCommits runs fn (a consumer's Run) until the fake reader has
// seen wantCommits commits, then cancels and returns Run's error.
func runUntilCommits(t *testing.T, r *fakeReader, wantCommits int, run func(context.Context) error) error {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	seen := 0
	r.commitFn = func(kafkago.Message) {
		seen++
		if seen >= wantCommits {
			cancel()
		}
	}
	done := make(chan error, 1)
	go func() { done <- run(ctx) }()
	select {
	case err := <-done:
		return err
	case <-time.After(10 * time.Second):
		t.Fatalf("Run did not finish; events=%v", r.log())
		return nil
	}
}

// End-to-end through the REAL storage consumer's Run: the tally step
// fails twice (transient DB error after the mutation was applied), then succeeds.
// The offset is committed once, after the success; state is consistent.
func TestStorageConsumer_Run_TransientFailuresRetriedThenCommittedOnce(t *testing.T) {
	h := newStorageHarness()
	h.flaky.failNext(2)
	reader := newFakeReader(registeredEvent(t, "evt-1", "LOC-1", "Z", "BULK"))
	h.consumer.Reader = reader

	err := runUntilCommits(t, reader, 1, h.consumer.Run)
	if err != context.Canceled {
		t.Fatalf("Run = %v, want context.Canceled", err)
	}

	if got, want := reader.log(), []string{"fetch:0", "commit:0"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("reader events = %v, want exactly one fetch and one commit %v", got, want)
	}
	if h.flaky.callCount() != 3 {
		t.Fatalf("tally mutated %d times, want 3 (fail, fail, succeed) for the SAME message", h.flaky.callCount())
	}
	if h.tally.Count("Z", "LOCATION", "BULK") != 1 {
		t.Fatalf("tally=%d (a failed attempt leaked or the retry double-counted), want 1", h.tally.Count("Z", "LOCATION", "BULK"))
	}
	if !h.processed.Has(storageName, "evt-1") {
		t.Fatal("event not recorded as processed after success")
	}
}

func TestLaborConsumer_Run_TransientFailuresRetriedThenCommittedOnce(t *testing.T) {
	h := newLaborHarness()
	h.flaky.failSaveAfter(0, 2)
	at := time.Date(2026, 10, 5, 6, 0, 0, 0, time.UTC)
	reader := newFakeReader(shiftPlanCommittedEvent(t, "evt-1", at, defaultShiftPlanData()))
	h.consumer.Reader = reader

	if err := runUntilCommits(t, reader, 1, h.consumer.Run); err != context.Canceled {
		t.Fatalf("Run = %v", err)
	}
	if got, want := reader.log(), []string{"fetch:0", "commit:0"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("reader events = %v, want %v", got, want)
	}
	if h.flaky.saveCalls != 3 {
		t.Fatalf("Save attempted %d times, want 3", h.flaky.saveCalls)
	}
	if got := laborQty(t, h, at); got != 400 {
		t.Fatalf("constraint = %v, want 400", got)
	}
}

// A deterministic bad message is committed past after ONE handling: no
// retry, no sleep, and the next message still flows (partition not blocked).
func TestStorageConsumer_Run_DeterministicBadMessageCommittedOnceNotRetried(t *testing.T) {
	h := newStorageHarness()
	reader := newFakeReader(
		[]byte("garbage, not a cloudevent"),
		registeredEvent(t, "evt-2", "LOC-2", "Z", "BULK"),
	)
	h.consumer.Reader = reader

	if err := runUntilCommits(t, reader, 2, h.consumer.Run); err != context.Canceled {
		t.Fatalf("Run = %v", err)
	}
	want := []string{"fetch:0", "commit:0", "fetch:1", "commit:1"}
	if got := reader.log(); !reflect.DeepEqual(got, want) {
		t.Fatalf("reader events = %v, want %v", got, want)
	}
	if h.uow.calls.Load() != 1 {
		t.Fatalf("units of work = %d, want 1 (only the valid message touches the DB)", h.uow.calls.Load())
	}
	if h.tally.Count("Z", "LOCATION", "BULK") != 1 {
		t.Fatal("the good message after the bad one was not applied")
	}
}

// Shutdown mid-failure: Run returns ctx.Err() and the offset is NOT
// committed, so a restart redelivers it (the processed-event claim made it
// safe either way).
func TestStorageConsumer_Run_CancelledWhileFailing_DoesNotCommit(t *testing.T) {
	h := newStorageHarness()
	h.flaky.failNext(1 << 30)
	reader := newFakeReader(registeredEvent(t, "evt-1", "LOC-1", "Z", "BULK"))
	h.consumer.Reader = reader

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- h.consumer.Run(ctx) }()

	deadline := time.Now().Add(10 * time.Second)
	for {
		if h.flaky.callCount() >= 3 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("handler was not retried")
		}
		time.Sleep(time.Millisecond)
	}
	cancel()
	if err := <-done; err != context.Canceled {
		t.Fatalf("Run = %v", err)
	}
	for _, e := range reader.log() {
		if e != "fetch:0" {
			t.Fatalf("unexpected reader event %q; a never-handled message must not be committed", e)
		}
	}
	if h.tally.Count("Z", "LOCATION", "BULK") != 0 || h.processed.Has(storageName, "evt-1") {
		t.Fatal("failed attempts leaked state")
	}
}
