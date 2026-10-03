package memory

import (
	"context"
	"sync"
)

// Snapshotter is implemented by the in-memory repositories that can
// capture their state and hand back a function restoring it. It is what
// lets UnitOfWork actually roll back in unit tests.
type Snapshotter interface {
	Snapshot() (restore func())
}

type uowKey struct{}

// UnitOfWork is the in-memory ports.UnitOfWork test double.
//
// With no participants (NewUnitOfWork()) it just calls fn(ctx): there is
// no real transaction and NOTHING is rolled back on error -- fine for
// tests that don't care about atomicity. Pass the in-memory repositories
// as participants (NewUnitOfWork(pcRepo, processed, tally)) and Do
// snapshots every one of them first and restores them all if fn returns an
// error, giving unit tests the same all-or-nothing behaviour as the
// Postgres adapter. Do calls are serialized (a coarse stand-in for
// isolation), and Do is re-entrant: a nested Do on a ctx that already
// carries one joins the outer unit of work.
type UnitOfWork struct {
	mu           sync.Mutex
	participants []Snapshotter
}

// NewUnitOfWork constructs a UnitOfWork rolling back participants on error.
func NewUnitOfWork(participants ...Snapshotter) *UnitOfWork {
	return &UnitOfWork{participants: participants}
}

// Do implements ports.UnitOfWork.
func (u *UnitOfWork) Do(ctx context.Context, fn func(ctx context.Context) error) error {
	if ctx.Value(uowKey{}) != nil {
		return fn(ctx)
	}
	u.mu.Lock()
	defer u.mu.Unlock()

	restores := make([]func(), 0, len(u.participants))
	for _, p := range u.participants {
		restores = append(restores, p.Snapshot())
	}

	committed := false
	defer func() {
		if committed {
			return
		}
		// Error or panic: undo everything fn did.
		for _, restore := range restores {
			restore()
		}
	}()

	if err := fn(context.WithValue(ctx, uowKey{}, struct{}{})); err != nil {
		return err
	}
	committed = true
	return nil
}
