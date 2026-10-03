package kafka

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"reflect"
	"sync"
	"testing"
	"time"

	kafkago "github.com/segmentio/kafka-go"
)

// loopRecorder is one shared, ordered log of everything the run loop does:
// fetches, handler attempts, offset commits and backoff sleeps. Asserting
// on that single sequence is what proves "commit only after success".
type loopRecorder struct {
	mu     sync.Mutex
	events []string
	sleeps []time.Duration
}

func (r *loopRecorder) add(format string, a ...any) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.events = append(r.events, fmt.Sprintf(format, a...))
}

func (r *loopRecorder) log() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]string(nil), r.events...)
}

type scriptedReader struct {
	rec       *loopRecorder
	msgs      []kafkago.Message
	next      int
	commitErr []error // consumed one per CommitMessages call
	fetchErr  error
}

func (r *scriptedReader) FetchMessage(ctx context.Context) (kafkago.Message, error) {
	if r.next < len(r.msgs) {
		m := r.msgs[r.next]
		r.next++
		r.rec.add("fetch:%d", m.Offset)
		return m, nil
	}
	if r.fetchErr != nil {
		return kafkago.Message{}, r.fetchErr
	}
	<-ctx.Done()
	return kafkago.Message{}, ctx.Err()
}

func (r *scriptedReader) CommitMessages(_ context.Context, msgs ...kafkago.Message) error {
	for _, m := range msgs {
		r.rec.add("commit:%d", m.Offset)
	}
	if len(r.commitErr) > 0 {
		err := r.commitErr[0]
		r.commitErr = r.commitErr[1:]
		return err
	}
	return nil
}

func (r *scriptedReader) Close() error { return nil }

func quietLogger() *slog.Logger { return slog.New(slog.NewTextHandler(io.Discard, nil)) }

func msgs(n int) []kafkago.Message {
	out := make([]kafkago.Message, n)
	for i := range out {
		out[i] = kafkago.Message{Offset: int64(i), Value: []byte{byte(i)}}
	}
	return out
}

// newLoop builds a loop whose handler fails the first failures[offset]
// attempts for each offset, then succeeds; sleeps are recorded, not slept.
func newLoop(rec *loopRecorder, reader Reader, failures map[int64]int) *consumeLoop {
	attempts := map[int64]int{}
	return &consumeLoop{
		reader: reader,
		logger: quietLogger(),
		name:   "test consumer",
		handle: func(_ context.Context, m kafkago.Message) error {
			attempts[m.Offset]++
			if attempts[m.Offset] <= failures[m.Offset] {
				rec.add("handle:%d:fail", m.Offset)
				return errors.New("transient")
			}
			rec.add("handle:%d:ok", m.Offset)
			return nil
		},
		sleep: func(_ context.Context, d time.Duration) error {
			rec.mu.Lock()
			rec.sleeps = append(rec.sleeps, d)
			rec.mu.Unlock()
			rec.add("sleep:%s", d)
			return nil
		},
	}
}

func runUntilCancelled(t *testing.T, l *consumeLoop, rec *loopRecorder, wantCommits int) error {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- l.run(ctx) }()

	deadline := time.After(5 * time.Second)
	for {
		commits := 0
		for _, e := range rec.log() {
			if len(e) > 6 && e[:6] == "commit" {
				commits++
			}
		}
		if commits >= wantCommits {
			cancel()
			break
		}
		select {
		case err := <-done:
			return err
		case <-deadline:
			t.Fatalf("timed out; events=%v", rec.log())
		case <-time.After(time.Millisecond):
		}
	}
	select {
	case err := <-done:
		return err
	case <-time.After(5 * time.Second):
		t.Fatal("run did not return after cancel")
		return nil
	}
}

// The headline run-loop proof (defect 1): the handler fails twice then
// succeeds -> the message is committed EXACTLY ONCE, AFTER the success,
// never after a failure, with 200ms then 400ms backoff.
func TestConsumeLoop_HandlerFailsTwiceThenSucceeds_CommitsOnceAfterSuccess(t *testing.T) {
	rec := &loopRecorder{}
	reader := &scriptedReader{rec: rec, msgs: msgs(1)}
	l := newLoop(rec, reader, map[int64]int{0: 2})

	err := runUntilCancelled(t, l, rec, 1)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("run returned %v, want context.Canceled", err)
	}

	want := []string{
		"fetch:0",
		"handle:0:fail", "sleep:200ms",
		"handle:0:fail", "sleep:400ms",
		"handle:0:ok",
		"commit:0",
	}
	if got := rec.log(); !reflect.DeepEqual(got, want) {
		t.Fatalf("event order\n got  %v\n want %v", got, want)
	}
}

// Backoff doubles from 200ms and is capped at 5s; the SAME message is
// retried throughout and nothing is committed until it finally succeeds.
func TestConsumeLoop_BackoffIsCappedAndNeverSkipsOrCommitsOnFailure(t *testing.T) {
	rec := &loopRecorder{}
	reader := &scriptedReader{rec: rec, msgs: msgs(1)}
	l := newLoop(rec, reader, map[int64]int{0: 9})

	if err := runUntilCancelled(t, l, rec, 1); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}

	wantSleeps := []time.Duration{
		200 * time.Millisecond, 400 * time.Millisecond, 800 * time.Millisecond, 1600 * time.Millisecond,
		3200 * time.Millisecond, 5 * time.Second, 5 * time.Second, 5 * time.Second, 5 * time.Second,
	}
	if !reflect.DeepEqual(rec.sleeps, wantSleeps) {
		t.Fatalf("sleeps\n got  %v\n want %v", rec.sleeps, wantSleeps)
	}
	log := rec.log()
	if log[len(log)-1] != "commit:0" {
		t.Fatalf("last event %q, want the single commit", log[len(log)-1])
	}
	for _, e := range log[:len(log)-1] {
		if e == "commit:0" {
			t.Fatal("committed before the handler succeeded")
		}
	}
}

// Cancelling while a message keeps failing must stop WITHOUT committing:
// the offset stays uncommitted so the message is redelivered on restart.
func TestConsumeLoop_CancelWhileFailing_NeverCommits(t *testing.T) {
	rec := &loopRecorder{}
	reader := &scriptedReader{rec: rec, msgs: msgs(2)}
	ctx, cancel := context.WithCancel(context.Background())
	l := newLoop(rec, reader, map[int64]int{0: 1 << 30})
	sleeps := 0
	l.sleep = func(ctx context.Context, d time.Duration) error {
		sleeps++
		if sleeps == 3 {
			cancel()
			return ctx.Err()
		}
		return nil
	}

	err := l.run(ctx)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("run = %v, want context.Canceled", err)
	}
	for _, e := range rec.log() {
		if e == "commit:0" || e == "commit:1" || e == "fetch:1" {
			t.Fatalf("event %q after a never-succeeding message 0; log=%v", e, rec.log())
		}
	}
}

// A handler that fails because ctx was cancelled (e.g. pgx returning
// context canceled) ends the loop at once: no sleep, no commit.
func TestConsumeLoop_HandlerErrorDuringShutdown_NoCommitNoSleep(t *testing.T) {
	rec := &loopRecorder{}
	reader := &scriptedReader{rec: rec, msgs: msgs(1)}
	ctx, cancel := context.WithCancel(context.Background())
	l := newLoop(rec, reader, nil)
	l.handle = func(ctx context.Context, m kafkago.Message) error {
		cancel()
		return ctx.Err()
	}
	if err := l.run(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("run = %v", err)
	}
	if got := rec.log(); !reflect.DeepEqual(got, []string{"fetch:0"}) {
		t.Fatalf("log = %v, want only the fetch", got)
	}
}

// The next message is not fetched until the current one is committed.
func TestConsumeLoop_MessagesAreProcessedStrictlyInOrder(t *testing.T) {
	rec := &loopRecorder{}
	reader := &scriptedReader{rec: rec, msgs: msgs(2)}
	l := newLoop(rec, reader, map[int64]int{0: 1, 1: 1})
	if err := runUntilCancelled(t, l, rec, 2); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	want := []string{
		"fetch:0", "handle:0:fail", "sleep:200ms", "handle:0:ok", "commit:0",
		"fetch:1", "handle:1:fail", "sleep:200ms", "handle:1:ok", "commit:1",
	}
	if got := rec.log(); !reflect.DeepEqual(got, want) {
		t.Fatalf("got  %v\nwant %v", got, want)
	}
}

// A failed offset commit is retried (the work is already durable); the
// handler is NOT run again for it.
func TestConsumeLoop_CommitFailureIsRetriedWithoutRehandling(t *testing.T) {
	rec := &loopRecorder{}
	reader := &scriptedReader{rec: rec, msgs: msgs(1), commitErr: []error{errors.New("broker unavailable")}}
	l := newLoop(rec, reader, nil)
	if err := runUntilCancelled(t, l, rec, 2); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	want := []string{"fetch:0", "handle:0:ok", "commit:0", "sleep:200ms", "commit:0"}
	if got := rec.log(); !reflect.DeepEqual(got, want) {
		t.Fatalf("got  %v\nwant %v", got, want)
	}
}

func TestConsumeLoop_FetchErrorEndsRun(t *testing.T) {
	rec := &loopRecorder{}
	boom := errors.New("reader closed")
	reader := &scriptedReader{rec: rec, fetchErr: boom}
	l := newLoop(rec, reader, nil)
	if err := l.run(context.Background()); !errors.Is(err, boom) {
		t.Fatalf("run = %v, want the fetch error", err)
	}
}

func TestRetryPolicy(t *testing.T) {
	d := RetryPolicy{}.withDefaults()
	if d.Initial != 200*time.Millisecond || d.Max != 5*time.Second {
		t.Fatalf("defaults = %+v", d)
	}
	custom := RetryPolicy{Initial: time.Second, Max: 3 * time.Second}.withDefaults()
	if custom.Initial != time.Second || custom.Max != 3*time.Second {
		t.Fatalf("custom overridden: %+v", custom)
	}
	if got := custom.next(time.Second); got != 2*time.Second {
		t.Fatalf("next(1s) = %v", got)
	}
	if got := custom.next(2 * time.Second); got != 3*time.Second {
		t.Fatalf("next(2s) = %v, want cap 3s", got)
	}
	if got := custom.next(3 * time.Second); got != 3*time.Second {
		t.Fatalf("next(3s) = %v, want cap 3s", got)
	}
}

func TestSleepCtx(t *testing.T) {
	if err := sleepCtx(context.Background(), time.Millisecond); err != nil {
		t.Fatalf("sleepCtx = %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	start := time.Now()
	if err := sleepCtx(ctx, time.Hour); !errors.Is(err, context.Canceled) {
		t.Fatalf("sleepCtx(cancelled) = %v", err)
	}
	if time.Since(start) > time.Second {
		t.Fatal("sleepCtx did not return promptly on cancel")
	}
}
