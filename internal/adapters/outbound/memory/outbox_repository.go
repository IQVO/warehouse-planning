package memory

import (
	"context"
	"fmt"
	"sync"

	"github.com/claudioed/warehouse-planning/internal/application/outbox"
)

type outboxRow struct {
	msg       outbox.Message
	published bool
	attempts  int
	lastError string
}

// OutboxRepo is an in-memory ports.OutboxRepository that is also the
// relay's Drain source, mirroring the Postgres outbox_events table. For
// tests and DATABASE_URL-less local dev only.
type OutboxRepo struct {
	mu   sync.Mutex
	rows []*outboxRow
}

// NewOutboxRepo constructs an empty OutboxRepo.
func NewOutboxRepo() *OutboxRepo { return &OutboxRepo{} }

// Insert appends msgs as unpublished rows, in order.
func (r *OutboxRepo) Insert(_ context.Context, msgs ...outbox.Message) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, m := range msgs {
		r.rows = append(r.rows, &outboxRow{msg: m})
	}
	return nil
}

// Messages returns every stored message in insertion order (published or
// not) -- a test inspection helper.
func (r *OutboxRepo) Messages() []outbox.Message {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]outbox.Message, len(r.rows))
	for i, row := range r.rows {
		out[i] = row.msg
	}
	return out
}

// Unpublished returns how many rows have not been published yet.
func (r *OutboxRepo) Unpublished() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	n := 0
	for _, row := range r.rows {
		if !row.published {
			n++
		}
	}
	return n
}

// LastError returns the last send error recorded on the i-th row ("" if
// none) -- a test inspection helper.
func (r *OutboxRepo) LastError(i int) string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.rows[i].lastError
}

// Drain sends up to limit unpublished rows to send, oldest first and one
// at a time, marking each published once send returned nil. It stops at
// the first send error (preserving per-key order), records it on that row
// and returns it, together with how many rows were published.
func (r *OutboxRepo) Drain(ctx context.Context, limit int, send func(context.Context, outbox.Message) error) (int, error) {
	r.mu.Lock()
	var pending []*outboxRow
	for _, row := range r.rows {
		if !row.published && len(pending) < limit {
			pending = append(pending, row)
		}
	}
	r.mu.Unlock()

	published := 0
	for _, row := range pending {
		err := send(ctx, row.msg)
		r.mu.Lock()
		row.attempts++
		if err != nil {
			row.lastError = err.Error()
			r.mu.Unlock()
			return published, fmt.Errorf("outbox drain: send %s: %w", row.msg.EventType, err)
		}
		row.published = true
		row.lastError = ""
		r.mu.Unlock()
		published++
	}
	return published, nil
}

// Snapshot implements Snapshotter.
func (r *OutboxRepo) Snapshot() func() {
	r.mu.Lock()
	defer r.mu.Unlock()
	saved := make([]*outboxRow, len(r.rows))
	for i, row := range r.rows {
		cp := *row
		saved[i] = &cp
	}
	return func() {
		r.mu.Lock()
		defer r.mu.Unlock()
		r.rows = saved
	}
}
