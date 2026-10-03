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
// same aggregate must never overtake a failed earlier one), records the
// error on that row, commits what was already sent and returns the error.
//
// Delivery is at-least-once: a crash between a successful send and the
// UPDATE republishes that row on the next pass -- with the SAME persisted
// event id, which is what lets consumers dedupe.
func (r *OutboxRepo) Drain(ctx context.Context, limit int, send func(context.Context, outbox.Message) error) (int, error) {
	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return 0, fmt.Errorf("begin outbox pass: %w", err)
	}
	defer func() { _ = tx.Rollback(context.WithoutCancel(ctx)) }()

	batch, err := claim(ctx, tx, limit)
	if err != nil {
		return 0, err
	}

	published := 0
	for _, row := range batch {
		if err := send(ctx, row.msg); err != nil {
			// Record the failure and keep what was already sent. The
			// UPDATE/COMMIT use a ctx that survives cancellation so a
			// shutdown mid-pass still persists the rows already sent.
			dctx := context.WithoutCancel(ctx)
			if _, uerr := tx.Exec(dctx, `UPDATE outbox_events SET attempts = attempts + 1, last_error = $2 WHERE id = $1`, row.id, err.Error()); uerr != nil {
				err = errors.Join(err, fmt.Errorf("record outbox failure: %w", uerr))
			}
			if cerr := tx.Commit(dctx); cerr != nil {
				err = errors.Join(err, fmt.Errorf("commit outbox pass: %w", cerr))
			}
			return published, fmt.Errorf("outbox row %d (%s): %w", row.id, row.msg.EventType, err)
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

func claim(ctx context.Context, tx pgx.Tx, limit int) ([]pendingRow, error) {
	rows, err := tx.Query(ctx, `
		SELECT id, event_id, topic, event_type, subject, key, dataschema, value, headers
		FROM outbox_events
		WHERE published_at IS NULL
		ORDER BY id
		LIMIT $1
		FOR UPDATE SKIP LOCKED
	`, limit)
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
