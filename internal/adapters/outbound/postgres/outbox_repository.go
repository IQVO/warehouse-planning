package postgres

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/claudioed/warehouse-planning/internal/application/outbox"
)

// OutboxRepo is the Postgres transactional outbox: Insert is the write
// side (ports.OutboxRepository), Drain is the relay's read side. It
// mirrors workforce-management's outbox_events table and relay claim loop.
type OutboxRepo struct {
	pool *pgxpool.Pool
}

// NewOutboxRepo constructs an OutboxRepo over pool.
func NewOutboxRepo(pool *pgxpool.Pool) *OutboxRepo { return &OutboxRepo{pool: pool} }

// headerJSON is the persisted shape of one Kafka header.
type headerJSON struct {
	Key   string `json:"key"`
	Value string `json:"value"`
}

func marshalHeaders(hs []outbox.Header) ([]byte, error) {
	out := make([]headerJSON, len(hs))
	for i, h := range hs {
		out[i] = headerJSON{Key: h.Key, Value: h.Value}
	}
	return json.Marshal(out)
}

func unmarshalHeaders(b []byte) ([]outbox.Header, error) {
	var in []headerJSON
	if err := json.Unmarshal(b, &in); err != nil {
		return nil, err
	}
	out := make([]outbox.Header, len(in))
	for i, h := range in {
		out[i] = outbox.Header{Key: h.Key, Value: h.Value}
	}
	return out, nil
}

// Insert stores msgs as unpublished rows. Called with a UnitOfWork ctx it
// runs on that transaction (no commit of its own), so the rows land
// atomically with the aggregate; outside a UnitOfWork the batch is its own
// transaction.
func (r *OutboxRepo) Insert(ctx context.Context, msgs ...outbox.Message) error {
	if len(msgs) == 0 {
		return nil
	}
	return inTx(ctx, r.pool, func(q querier) error {
		for _, m := range msgs {
			headers, err := marshalHeaders(m.Headers)
			if err != nil {
				return fmt.Errorf("marshal outbox headers for %s: %w", m.EventType, err)
			}
			if _, err := q.Exec(ctx, `
				INSERT INTO outbox_events (event_id, topic, event_type, subject, key, dataschema, value, headers)
				VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
			`, m.EventID, m.Topic, m.EventType, m.Subject, m.Key, m.DataSchema, m.Value, headers); err != nil {
				return fmt.Errorf("enqueue outbox event %s: %w", m.EventType, err)
			}
		}
		return nil
	})
}

type pendingRow struct {
	id  int64
	msg outbox.Message
}

// Drain is the relay's single pass: it claims up to limit unpublished rows
// under a row lock (FOR UPDATE SKIP LOCKED, so two overlapping relays never
// claim the same row), sends them ONE AT A TIME in id order via send and
// marks each published as it goes. It returns how many rows were
// published. On the first send failure it stops (a later event for the
// same aggregate must never overtake a failed earlier one that is still
// being retried), records the error on that row, commits what was already
// sent and returns an *outbox.SendFailure carrying the row's new attempts.
//
// With maxAttempts > 0 a row whose attempts already reached the cap is not
// claimed (gap fix D12, ADR 0013): it stays in outbox_events with attempts
// and last_error for inspection, and later rows -- including rows with the
// same key -- overtake it. 0 claims every unpublished row (unlimited
// retries).
//
// Delivery is at-least-once: a crash between a successful send and the
// UPDATE republishes that row on the next pass -- with the SAME persisted
// event id, which is what lets consumers dedupe.
func (r *OutboxRepo) Drain(ctx context.Context, limit, maxAttempts int, send func(context.Context, outbox.Message) error) (int, error) {
	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return 0, fmt.Errorf("begin outbox pass: %w", err)
	}
	defer func() { _ = tx.Rollback(context.WithoutCancel(ctx)) }()

	batch, err := claim(ctx, tx, limit, maxAttempts)
	if err != nil {
		return 0, err
	}

	published := 0
	for _, row := range batch {
		if err := send(ctx, row.msg); err != nil {
			return published, recordFailure(ctx, tx, row, err)
		}
		if _, err := tx.Exec(ctx, `UPDATE outbox_events SET published_at = now(), attempts = attempts + 1, last_error = NULL WHERE id = $1`, row.id); err != nil {
			return published, fmt.Errorf("mark outbox row %d published: %w", row.id, err)
		}
		published++
	}
	if err := tx.Commit(ctx); err != nil {
		return published, fmt.Errorf("commit outbox pass: %w", err)
	}
	return published, nil
}

// recordFailure stores the send error on the row, commits the pass (keeping
// the rows already sent) and returns the failure with the row's new attempt
// count. The UPDATE/COMMIT use a ctx that survives cancellation so a
// shutdown mid-pass still persists the rows already sent.
func recordFailure(ctx context.Context, tx pgx.Tx, row pendingRow, sendErr error) error {
	dctx := context.WithoutCancel(ctx)
	failure := &outbox.SendFailure{RowID: row.id, Message: row.msg, Err: sendErr}
	if uerr := tx.QueryRow(dctx, `UPDATE outbox_events SET attempts = attempts + 1, last_error = $2 WHERE id = $1 RETURNING attempts`,
		row.id, sendErr.Error()).Scan(&failure.Attempts); uerr != nil {
		failure.Err = errors.Join(failure.Err, fmt.Errorf("record outbox failure: %w", uerr))
	}
	if cerr := tx.Commit(dctx); cerr != nil {
		failure.Attempts = 0
		failure.Err = errors.Join(failure.Err, fmt.Errorf("commit outbox pass: %w", cerr))
	}
	return failure
}

func claim(ctx context.Context, tx pgx.Tx, limit, maxAttempts int) ([]pendingRow, error) {
	var capFilter string
	args := []any{limit}
	if maxAttempts > 0 {
		capFilter = "AND attempts < $2"
		args = append(args, maxAttempts)
	}
	rows, err := tx.Query(ctx, `
		SELECT id, event_id, topic, event_type, subject, key, dataschema, value, headers
		FROM outbox_events
		WHERE published_at IS NULL `+capFilter+`
		ORDER BY id
		LIMIT $1
		FOR UPDATE SKIP LOCKED
	`, args...)
	if err != nil {
		return nil, fmt.Errorf("claim outbox rows: %w", err)
	}
	defer rows.Close()

	var batch []pendingRow
	for rows.Next() {
		var (
			p       pendingRow
			headers []byte
		)
		if err := rows.Scan(&p.id, &p.msg.EventID, &p.msg.Topic, &p.msg.EventType, &p.msg.Subject, &p.msg.Key, &p.msg.DataSchema, &p.msg.Value, &headers); err != nil {
			return nil, fmt.Errorf("scan outbox row: %w", err)
		}
		if p.msg.Headers, err = unmarshalHeaders(headers); err != nil {
			return nil, fmt.Errorf("decode outbox row %d headers: %w", p.id, err)
		}
		batch = append(batch, p)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("read outbox rows: %w", err)
	}
	return batch, nil
}
