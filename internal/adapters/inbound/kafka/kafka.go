// Package kafka holds this service's INBOUND Kafka consumers (Phase 3):
// LaborCapacityConsumer (workforce-management's ShiftPlanCommitted) and
// StorageCapacityConsumer (facility-layout's
// LocationSlotRegistered/LocationSlotDecommissioned). Both decode
// CloudEvents 1.0 only via internal/adapters/kafka/cloudevents, dispatch
// on the FULL `type` string, and call the EXISTING
// usecases.RegisterProcessCapacityConstraint to upsert a ProcessCapacity
// constraint -- neither consumer duplicates that aggregate logic. See
// docs/adr/0001-warehouse-planning-bounded-context.md's Addendum and
// .claude/rules/integration-events.md for the confirmed upstream
// contracts.
//
// Delivery guarantee: AT-LEAST-ONCE with an ATOMIC effect. Each message is
// handled inside ONE ports.UnitOfWork (processed-event claim + every
// side effect commit or roll back together) and its offset is committed
// only AFTER HandleMessage returned nil. HandleMessage returns a non-nil
// error ONLY for transient/infrastructure failures; the run loop then
// retries the SAME message with capped exponential backoff and never
// commits past it. Deterministic problems (not CloudEvents, unknown type,
// malformed payload, missing fields, domain validation failure) return nil.
package kafka

import (
	"context"
	"log/slog"
	"time"

	kafkago "github.com/segmentio/kafka-go"
)

// Reader is the subset of *kafkago.Reader a consumer needs, so unit tests
// never need a live broker. FetchMessage/CommitMessages (not ReadMessage)
// because ReadMessage with a GroupID auto-commits the offset the moment it
// returns, i.e. before the message has been handled -- a failed handling
// would then never be redelivered. Offsets are committed explicitly, after
// success only.
type Reader interface {
	FetchMessage(ctx context.Context) (kafkago.Message, error)
	CommitMessages(ctx context.Context, msgs ...kafkago.Message) error
	Close() error
}

// readerConfig builds a kafka-go reader configuration shared by both
// consumers. GroupID always comes from the caller (an env var at the
// composition root -- never a literal here), satisfying
// internal/architecture/fitness_test.go's
// TestKafkaConsumerGroupNeverHardcodedInline.
//
// CommitInterval is deliberately left UNSET: with a GroupID kafka-go then
// commits synchronously inside CommitMessages, which the run loop calls
// only after a message was handled successfully. Setting it would
// batch/async the commit and reintroduce a window where an offset moves
// without the work being durable.
//
// kafkago.NewReader itself does not dial the broker synchronously -- the
// real TCP connection is only established the first time FetchMessage
// runs inside the consumer's own background goroutine (see cmd/api's
// composition root), so constructing a Reader here never blocks process
// startup on Kafka being reachable.
func readerConfig(brokers []string, topic, groupID string) kafkago.ReaderConfig {
	return kafkago.ReaderConfig{
		Brokers: brokers,
		Topic:   topic,
		GroupID: groupID,
	}
}

func defaultLogger(logger *slog.Logger) *slog.Logger {
	if logger == nil {
		return slog.Default()
	}
	return logger
}

// RetryPolicy is the capped exponential backoff the run loop applies while
// the SAME message keeps failing with a transient error. Zero fields fall
// back to DefaultRetryInitial / DefaultRetryMax.
type RetryPolicy struct {
	Initial time.Duration
	Max     time.Duration
}

// Default backoff: 200ms, 400ms, 800ms ... capped at 5s.
const (
	DefaultRetryInitial = 200 * time.Millisecond
	DefaultRetryMax     = 5 * time.Second
)

func (p RetryPolicy) withDefaults() RetryPolicy {
	if p.Initial <= 0 {
		p.Initial = DefaultRetryInitial
	}
	if p.Max <= 0 {
		p.Max = DefaultRetryMax
	}
	return p
}

// next doubles d, capped at p.Max.
func (p RetryPolicy) next(d time.Duration) time.Duration {
	d *= 2
	if d > p.Max {
		return p.Max
	}
	return d
}

// sleepFunc waits d or until ctx is done (returning ctx.Err()). A field so
// tests can record the backoff sequence without really sleeping.
type sleepFunc func(ctx context.Context, d time.Duration) error

func sleepCtx(ctx context.Context, d time.Duration) error {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-t.C:
		return nil
	}
}

// consumeLoop is the at-least-once run loop shared by both consumers.
type consumeLoop struct {
	reader Reader
	// handle applies one message; non-nil means transient failure => retry.
	handle func(ctx context.Context, msg kafkago.Message) error
	logger *slog.Logger
	name   string
	retry  RetryPolicy
	sleep  sleepFunc
}

// run fetches one message at a time and does not fetch the next until the
// current one was handled successfully AND its offset committed. It
// returns only when ctx is cancelled or FetchMessage itself fails.
func (l *consumeLoop) run(ctx context.Context) error {
	policy := l.retry.withDefaults()
	sleep := l.sleep
	if sleep == nil {
		sleep = sleepCtx
	}
	for {
		msg, err := l.reader.FetchMessage(ctx)
		if err != nil {
			return err
		}
		if err := l.retryUntilOK(ctx, policy, sleep, "handling", msg, func() error {
			return l.handle(ctx, msg)
		}); err != nil {
			return err
		}
		// Commit ONLY now. A commit that fails is retried too: the work is
		// already durable, and if the process dies first the redelivery is
		// skipped by the processed-event claim.
		if err := l.retryUntilOK(ctx, policy, sleep, "offset commit", msg, func() error {
			return l.reader.CommitMessages(ctx, msg)
		}); err != nil {
			return err
		}
	}
}

// retryUntilOK runs op until it returns nil, sleeping with capped
// exponential backoff between attempts. It gives up (returning ctx.Err())
// only when ctx is cancelled. It NEVER skips: a message that cannot be
// handled blocks its partition (and is logged at ERROR every attempt)
// rather than being dropped, because a drop is silent data loss.
func (l *consumeLoop) retryUntilOK(ctx context.Context, p RetryPolicy, sleep sleepFunc, what string, msg kafkago.Message, op func() error) error {
	delay := p.Initial
	for attempt := 1; ; attempt++ {
		err := op()
		if err == nil {
			return nil
		}
		if ctx.Err() != nil {
			return ctx.Err()
		}
		l.logger.ErrorContext(ctx, l.name+" "+what+" failed; retrying the same message",
			"error", err, "partition", msg.Partition, "offset", msg.Offset,
			"attempt", attempt, "retry_in", delay)
		if err := sleep(ctx, delay); err != nil {
			return err
		}
		delay = p.next(delay)
	}
}
