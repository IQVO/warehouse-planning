package memory_test

import (
	"sync"
	"testing"
	"time"

	"github.com/claudioed/warehouse-planning/internal/adapters/outbound/memory"
	"github.com/claudioed/warehouse-planning/internal/adapters/outbound/repocontract"
	"github.com/claudioed/warehouse-planning/internal/application/ports"
)

// movingClock is a test clock: advance moves time, the store ages with it.
type movingClock struct {
	mu sync.Mutex
	t  time.Time
}

func (c *movingClock) now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.t
}

func (c *movingClock) advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.t = c.t.Add(d)
}

var _ ports.IdempotencyStore = (*memory.IdempotencyStore)(nil)

// The in-memory double shows the retention behaviour the Postgres store
// shows (postgres/idempotency_purge_integration_test.go runs the same suite).
func TestTheIdempotencyDoubleSatisfiesThePurgeContract(t *testing.T) {
	repocontract.RunIdempotencyPurge(t, func(t *testing.T) repocontract.IdempotencyFixture {
		clock := &movingClock{t: time.Date(2026, 11, 23, 8, 0, 0, 0, time.UTC)}
		store := memory.NewIdempotencyStoreClock(clock.now)
		return repocontract.IdempotencyFixture{
			Store: store,
			Age:   func(_ *testing.T, d time.Duration) { clock.advance(d) },
		}
	})
}
