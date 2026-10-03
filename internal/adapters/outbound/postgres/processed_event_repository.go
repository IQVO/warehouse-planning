package postgres

import (
	"context"

	"github.com/jackc/pgx/v5/pgxpool"
)

// ProcessedEventRepo is a pgxpool-backed implementation of
// ports.ProcessedEventRepository (Phase 3 Kafka consumer idempotency).
type ProcessedEventRepo struct {
	pool *pgxpool.Pool
}

// NewProcessedEventRepo constructs a ProcessedEventRepo over pool.
func NewProcessedEventRepo(pool *pgxpool.Pool) *ProcessedEventRepo {
	return &ProcessedEventRepo{pool: pool}
}

// Claim records (consumer, eventID) as processed, returning false if it
// was already recorded by an earlier call. Called inside a
// ports.UnitOfWork (the consumers' case) the INSERT is part of the SAME
// transaction as the event's side effects, so a rollback un-claims it and
// a redelivery is processed, not skipped. A concurrent claim of the same
// key blocks on the unique index until the other transaction ends.
func (r *ProcessedEventRepo) Claim(ctx context.Context, consumer, eventID string) (bool, error) {
	tag, err := queryFor(ctx, r.pool).Exec(ctx, `
		INSERT INTO processed_events (consumer, event_id)
		VALUES ($1, $2)
		ON CONFLICT (consumer, event_id) DO NOTHING
	`, consumer, eventID)
	if err != nil {
		return false, err
	}
	return tag.RowsAffected() == 1, nil
}
