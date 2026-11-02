package postgres

import (
	"context"
	"errors"
	"fmt"

	"github.com/claudioed/warehouse-planning/internal/domain/idempotency"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/claudioed/warehouse-planning/internal/application/ports"
)

// IdempotencyStore is the Postgres ports.IdempotencyStore. It must run inside
// a ports.UnitOfWork: Lock takes a transaction-scoped advisory lock on the key
// so concurrent same-key requests are serialised and the loser replays the
// winner's committed answer.
type IdempotencyStore struct {
	pool *pgxpool.Pool
}

// NewIdempotencyStore constructs an IdempotencyStore over pool.
func NewIdempotencyStore(pool *pgxpool.Pool) *IdempotencyStore {
	return &IdempotencyStore{pool: pool}
}

// Lock implements ports.IdempotencyStore.
func (s *IdempotencyStore) Lock(ctx context.Context, key string) (*idempotency.Record, error) {
	q := queryFor(ctx, s.pool)
	if _, err := q.Exec(ctx, `SELECT pg_advisory_xact_lock(hashtextextended($1, 0))`, key); err != nil {
		return nil, fmt.Errorf("lock idempotency key: %w", err)
	}
	rec := idempotency.Record{Key: key}
	err := q.QueryRow(ctx, `
		SELECT fingerprint, status, content_type, location, etag, body
		FROM idempotency_keys
		WHERE key = $1 AND created_at > now() - make_interval(secs => $2)`,
		key, ports.IdempotencyRetention.Seconds(),
	).Scan(&rec.Fingerprint, &rec.Status, &rec.ContentType, &rec.Location, &rec.ETag, &rec.Body)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("read idempotency key: %w", err)
	}
	return &rec, nil
}

// Save implements ports.IdempotencyStore.
func (s *IdempotencyStore) Save(ctx context.Context, rec idempotency.Record) error {
	_, err := queryFor(ctx, s.pool).Exec(ctx, `
		INSERT INTO idempotency_keys (key, fingerprint, status, content_type, location, etag, body)
		VALUES ($1, $2, $3, $4, $5, $6, $7)
		ON CONFLICT (key) DO UPDATE SET
			fingerprint = EXCLUDED.fingerprint, status = EXCLUDED.status,
			content_type = EXCLUDED.content_type, location = EXCLUDED.location,
			etag = EXCLUDED.etag, body = EXCLUDED.body, created_at = now()`,
		rec.Key, rec.Fingerprint, rec.Status, rec.ContentType, rec.Location, rec.ETag, rec.Body)
	if err != nil {
		return fmt.Errorf("save idempotency key: %w", err)
	}
	return nil
}
