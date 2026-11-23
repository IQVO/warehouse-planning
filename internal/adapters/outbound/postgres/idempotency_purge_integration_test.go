//go:build integration

package postgres_test

import (
	"context"
	"testing"
	"time"

	"github.com/claudioed/warehouse-planning/internal/adapters/outbound/postgres"
	"github.com/claudioed/warehouse-planning/internal/adapters/outbound/repocontract"
)

// The Postgres store shows the retention behaviour the in-memory double
// shows (memory/idempotency_purge_test.go runs the same suite): Age moves
// created_at back, the only way to reach an expired row without waiting a
// day.
func TestTheIdempotencyStoreSatisfiesThePurgeContract(t *testing.T) {
	repocontract.RunIdempotencyPurge(t, func(t *testing.T) repocontract.IdempotencyFixture {
		pool := newMigratedPool(t)
		return repocontract.IdempotencyFixture{
			Store: postgres.NewIdempotencyStore(pool),
			Age: func(t *testing.T, d time.Duration) {
				t.Helper()
				if _, err := pool.Exec(context.Background(),
					`UPDATE idempotency_keys SET created_at = created_at - make_interval(secs => $1)`, d.Seconds()); err != nil {
					t.Fatalf("age stored rows: %v", err)
				}
			},
		}
	})
}
