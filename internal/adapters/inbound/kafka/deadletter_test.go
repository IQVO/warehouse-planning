package kafka

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"testing"
	"time"

	kafkago "github.com/segmentio/kafka-go"
)

func quietDeadLetterLogger() *slog.Logger { return slog.New(slog.NewTextHandler(io.Discard, nil)) }

// TestHandleWithDeadLetter_ExhaustsAttemptsThenDeadLetters proves the fix
// for the "retries indefinitely" bug: a transient failure is retried
// EXACTLY maxAttempts times (not forever), then dead-lettered once, and
// run() moves on (committing the offset) instead of blocking the
// partition forever.
func TestHandleWithDeadLetter_ExhaustsAttemptsThenDeadLetters(t *testing.T) {
	var handleCalls int
	var dlqCalls []error
	injected := errors.New("transient")

	l := &consumeLoop{
		logger: quietDeadLetterLogger(),
		name:   "test",
		handle: func(context.Context, kafkago.Message) error {
			handleCalls++
			return injected
		},
		sleep:       func(context.Context, time.Duration) error { return nil },
		maxAttempts: 3,
		deadLetter: func(_ context.Context, _ kafkago.Message, cause error) error {
			dlqCalls = append(dlqCalls, cause)
			return nil
		},
	}

	err := l.handleWithDeadLetter(context.Background(), RetryPolicy{}.withDefaults(), l.sleep, kafkago.Message{Offset: 7})
	if err != nil {
		t.Fatalf("handleWithDeadLetter = %v, want nil (dead-lettered, loop moves on)", err)
	}
	if handleCalls != 3 {
		t.Fatalf("handle called %d times, want exactly maxAttempts=3", handleCalls)
	}
	if len(dlqCalls) != 1 || !errors.Is(dlqCalls[0], injected) {
		t.Fatalf("dead-letter calls = %v, want exactly one carrying the injected error", dlqCalls)
	}
}

// A handler that succeeds before exhausting maxAttempts never reaches the
// dead-letter path.
func TestHandleWithDeadLetter_SucceedsBeforeExhaustion_NeverDeadLetters(t *testing.T) {
	attempt := 0
	dlqCalled := false
	l := &consumeLoop{
		logger: quietDeadLetterLogger(),
		name:   "test",
		handle: func(context.Context, kafkago.Message) error {
			attempt++
			if attempt < 3 {
				return errors.New("transient")
			}
			return nil
		},
		sleep:       func(context.Context, time.Duration) error { return nil },
		maxAttempts: 5,
		deadLetter: func(context.Context, kafkago.Message, error) error {
			dlqCalled = true
			return nil
		},
	}
	if err := l.handleWithDeadLetter(context.Background(), RetryPolicy{}.withDefaults(), l.sleep, kafkago.Message{}); err != nil {
		t.Fatalf("handleWithDeadLetter = %v, want nil", err)
	}
	if attempt != 3 {
		t.Fatalf("handle called %d times, want 3 (fail, fail, succeed)", attempt)
	}
	if dlqCalled {
		t.Fatal("dead-letter must not be called when handle eventually succeeds")
	}
}

// A dead-letter PUBLISH failure is itself retried (never silently
// dropping the message); once it succeeds, the loop returns nil.
func TestHandleWithDeadLetter_PublishFailureIsRetried(t *testing.T) {
	dlqAttempts := 0
	l := &consumeLoop{
		logger:      quietDeadLetterLogger(),
		name:        "test",
		handle:      func(context.Context, kafkago.Message) error { return errors.New("transient") },
		sleep:       func(context.Context, time.Duration) error { return nil },
		retry:       RetryPolicy{Initial: time.Millisecond, Max: time.Millisecond},
		maxAttempts: 1,
		deadLetter: func(context.Context, kafkago.Message, error) error {
			dlqAttempts++
			if dlqAttempts < 3 {
				return errors.New("dlq broker blip")
			}
			return nil
		},
	}
	err := l.handleWithDeadLetter(context.Background(), RetryPolicy{Initial: time.Millisecond, Max: time.Millisecond}, l.sleep, kafkago.Message{})
	if err != nil {
		t.Fatalf("handleWithDeadLetter = %v, want nil once the DLQ publish finally succeeds", err)
	}
	if dlqAttempts != 3 {
		t.Fatalf("dead-letter publish attempted %d times, want 3 (fail, fail, succeed)", dlqAttempts)
	}
}

// Cancelling mid-retry (before maxAttempts is reached) must stop without
// dead-lettering: the offset stays uncommitted so the message is
// redelivered on restart, exactly as the unbounded path already behaves.
func TestHandleWithDeadLetter_CancelledMidRetry_NeverDeadLetters(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	dlqCalled := false
	l := &consumeLoop{
		logger: quietDeadLetterLogger(),
		name:   "test",
		handle: func(context.Context, kafkago.Message) error {
			cancel()
			return errors.New("transient")
		},
		sleep:       func(context.Context, time.Duration) error { return nil },
		maxAttempts: 5,
		deadLetter: func(context.Context, kafkago.Message, error) error {
			dlqCalled = true
			return nil
		},
	}
	err := l.handleWithDeadLetter(ctx, RetryPolicy{}.withDefaults(), l.sleep, kafkago.Message{})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("handleWithDeadLetter = %v, want context.Canceled", err)
	}
	if dlqCalled {
		t.Fatal("a cancelled shutdown must never dead-letter")
	}
}

// With no maxAttempts/deadLetter configured (the zero value -- every
// pre-existing consumeLoop caller, and AnalyticsConsumer), behaviour is
// UNCHANGED: handle is retried forever on a transient failure.
func TestHandleWithDeadLetter_ZeroValuePreservesUnboundedRetry(t *testing.T) {
	calls := 0
	ctx, cancel := context.WithCancel(context.Background())
	l := &consumeLoop{
		logger: quietDeadLetterLogger(),
		name:   "test",
		handle: func(context.Context, kafkago.Message) error {
			calls++
			if calls == 10 {
				cancel()
			}
			return errors.New("transient")
		},
		sleep: func(context.Context, time.Duration) error { return nil },
	}
	err := l.handleWithDeadLetter(ctx, RetryPolicy{}.withDefaults(), l.sleep, kafkago.Message{})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("handleWithDeadLetter = %v, want context.Canceled", err)
	}
	if calls != 10 {
		t.Fatalf("handle called %d times, want 10 (no bound without maxAttempts/deadLetter)", calls)
	}
}
