package ports

import (
	"context"
	"time"

	"github.com/claudioed/warehouse-planning/internal/domain/idempotency"
)

// IdempotencyRetention is how long a stored answer is replayable.
const IdempotencyRetention = 24 * time.Hour

// IdempotencyStore keeps one record per Idempotency-Key, looked up by key
// ALONE (the key is global, not per route). Both methods MUST be called with
// the ctx of an open ports.UnitOfWork so the key row commits in the SAME
// transaction as the aggregate change and its outbox rows.
type IdempotencyStore interface {
	// Lock serialises concurrent requests carrying key until the unit of
	// work ends, then returns the stored record, or nil when the key is
	// unseen (or its record is older than IdempotencyRetention).
	Lock(ctx context.Context, key string) (*idempotency.Record, error)
	// Save stores rec (replacing an expired record of the same key).
	Save(ctx context.Context, rec idempotency.Record) error
}
