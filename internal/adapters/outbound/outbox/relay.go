// Package outbox is the transactional-outbox relay: a background loop that
// drains unpublished outbox rows (written in the same transaction as the
// aggregate) to a Sink -- Kafka in production (EVENT_PUBLISHER=kafka) or a
// logger (the default, which keeps tests and local dev broker-free).
//
// The shape mirrors workforce-management's OutboxRelay (ADR 0016): drain
// every interval (default 1s), a full batch is followed immediately by
// another pass, a failed pass is logged and retried after the interval, and
// delivery is at-least-once with the CloudEvents id persisted at encode
// time so a retry republishes the same id.
package outbox

import (
	"context"
	"errors"
	"log/slog"
	"time"

	"github.com/claudioed/warehouse-planning/internal/application/outbox"
)

// Defaults for the relay's tuning knobs.
const (
	DefaultInterval  = time.Second
	DefaultBatchSize = 100
	// DefaultMaxAttempts is the attempt cap a composition root uses when
	// OUTBOX_MAX_ATTEMPTS is unset (fleet convention, gap fix D12, ADR
	// 0013). The relay's own zero value stays 0 (unlimited), so the cap is
	// always an explicit WithMaxAttempts at the composition root.
	DefaultMaxAttempts = 20
)

// AttemptCapReachedMsg is the ERROR log message written once for a row whose
// failure reached the attempt cap.
const AttemptCapReachedMsg = "outbox relay: row reached the attempt cap and is excluded from future claims; re-arm it with attempts = 0 after fixing the cause"

// Store is the relay's view of the outbox table: Drain claims up to limit
// unpublished rows in id order -- skipping rows that already failed
// maxAttempts times when maxAttempts > 0 -- calls send for each one at a
// time, marks a row published only after send returned nil, and stops at
// the first send error, which it returns as an *outbox.SendFailure.
// postgres.OutboxRepo and memory.OutboxRepo implement it.
type Store interface {
	Drain(ctx context.Context, limit, maxAttempts int, send func(context.Context, outbox.Message) error) (int, error)
}

// Sink is where drained messages go.
type Sink interface {
	Send(ctx context.Context, msgs ...outbox.Message) error
}

// Relay drains a Store into a Sink.
type Relay struct {
	store       Store
	sink        Sink
	logger      *slog.Logger
	interval    time.Duration
	batchSize   int
	maxAttempts int
}

// Option customises a Relay.
type Option func(*Relay)

// WithInterval sets how long the relay sleeps after a pass that did not
// fill a batch (or failed). Non-positive values are ignored.
func WithInterval(d time.Duration) Option {
	return func(r *Relay) {
		if d > 0 {
			r.interval = d
		}
	}
}

// WithBatchSize caps how many rows one pass claims. Non-positive values
// are ignored.
func WithBatchSize(n int) Option {
	return func(r *Relay) {
		if n > 0 {
			r.batchSize = n
		}
	}
}

// WithMaxAttempts caps how many times a row may fail before it is excluded
// from future claims (gap fix D12, ADR 0013). The excluded row stays in the
// outbox with its attempts and last error for inspection, the failure that
// reaches the cap is logged at ERROR with the row's id, topic, type and key,
// and later rows -- including rows with the same key -- overtake it. 0 means
// unlimited (the default); negative values are ignored. Every cmd/* that
// builds a relay must pass it (fitness test TestOutboxRelaysPassAttemptCap).
func WithMaxAttempts(n int) Option {
	return func(r *Relay) {
		if n >= 0 {
			r.maxAttempts = n
		}
	}
}

// NewRelay constructs a relay draining store into sink.
func NewRelay(store Store, sink Sink, logger *slog.Logger, opts ...Option) *Relay {
	if logger == nil {
		logger = slog.Default()
	}
	r := &Relay{store: store, sink: sink, logger: logger, interval: DefaultInterval, batchSize: DefaultBatchSize}
	for _, o := range opts {
		o(r)
	}
	return r
}

// RelayOnce performs a single pass and returns how many rows it published.
// When the pass failed on a row whose failure count reached the attempt
// cap, that row is logged once at ERROR (AttemptCapReachedMsg).
func (r *Relay) RelayOnce(ctx context.Context) (int, error) {
	n, err := r.store.Drain(ctx, r.batchSize, r.maxAttempts, func(ctx context.Context, m outbox.Message) error {
		return r.sink.Send(ctx, m)
	})
	r.logAttemptCap(ctx, err)
	return n, err
}

// logAttemptCap logs the row whose failure has just reached the cap: from
// the next pass on it is excluded from claims and stays in the outbox until
// an operator re-arms it (UPDATE outbox_events SET attempts = 0 WHERE id = ...).
func (r *Relay) logAttemptCap(ctx context.Context, err error) {
	var sf *outbox.SendFailure
	if r.maxAttempts == 0 || !errors.As(err, &sf) || sf.Attempts < r.maxAttempts {
		return
	}
	r.logger.ErrorContext(ctx, AttemptCapReachedMsg,
		"outbox_id", sf.RowID, "topic", sf.Message.Topic, "event_type", sf.Message.EventType,
		"key", string(sf.Message.Key), "attempts", sf.Attempts, "max_attempts", r.maxAttempts, "error", sf.Err)
}

// Run drains the outbox until ctx is cancelled and then returns ctx.Err().
// A pass that publishes a full batch is followed immediately by another
// (there is probably more waiting); any other pass sleeps for the interval.
// A failing pass is logged and retried after the interval -- the rows stay
// unpublished, so nothing is lost. Run never dials anything itself: a
// broker that is down at boot just makes passes fail until it is up.
func (r *Relay) Run(ctx context.Context) error {
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		n, err := r.RelayOnce(ctx)
		if err != nil && !errors.Is(err, context.Canceled) {
			r.logger.ErrorContext(ctx, "outbox relay pass failed", "error", err)
		}
		if n == r.batchSize && err == nil {
			continue
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(r.interval):
		}
	}
}

// LogSink is the EVENT_PUBLISHER=log sink: it logs each message instead of
// sending it anywhere, so the outbox still drains and nothing needs a
// broker.
type LogSink struct {
	Logger *slog.Logger
}

// Send logs msgs and reports success.
func (s LogSink) Send(ctx context.Context, msgs ...outbox.Message) error {
	logger := s.Logger
	if logger == nil {
		logger = slog.Default()
	}
	for _, m := range msgs {
		logger.InfoContext(ctx, "event published (log sink)",
			"topic", m.Topic, "type", m.EventType, "subject", m.Subject, "id", m.EventID)
	}
	return nil
}
