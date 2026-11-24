package repocontract

import (
	"testing"
	"time"

	"github.com/claudioed/warehouse-planning/internal/application/ports"
	"github.com/claudioed/warehouse-planning/internal/domain/idempotency"
)

// IdempotencyFixture is one ports.IdempotencyStore over ONE empty database.
type IdempotencyFixture struct {
	Store ports.IdempotencyStore
	// Age makes every answer stored so far d older (the memory double
	// advances its clock, Postgres moves created_at back): the only way a
	// test reaches an expired row without waiting a day. A negative d
	// makes stored answers younger again.
	Age func(t *testing.T, d time.Duration)
}

// IdempotencyFactory returns a fresh, empty IdempotencyFixture and registers
// its own cleanup with t.
type IdempotencyFactory func(t *testing.T) IdempotencyFixture

// RunIdempotencyPurge is the retention contract of
// ports.IdempotencyStore.PurgeExpired (the replay contract of the middleware
// lives in the inbound adapter's own tests): the memory double and the
// Postgres store both run it.
func RunIdempotencyPurge(t *testing.T, factory IdempotencyFactory) {
	t.Helper()
	cases := []struct {
		name string
		run  func(*testing.T, IdempotencyFixture)
	}{
		{"deletes expired keeps live", purgeKeepsLive},
		{"one batch per pass", purgeBatches},
		{"oldest first", purgeOldestFirst},
		{"no budget deletes nothing", purgeWithoutBudget},
		{"an expired answer never replays", expiredNeverReplays},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) { c.run(t, factory(t)) })
	}
}

// purgeKeepsLive: a row older than the retention goes, a row a minute short
// of it and a fresh one stay and replay; a second pass has nothing to delete.
func purgeKeepsLive(t *testing.T, f IdempotencyFixture) {
	putAnswer(t, f, "old", "v-old")
	f.Age(t, ports.IdempotencyRetention+time.Minute)
	putAnswer(t, f, "edge", "v-edge")
	f.Age(t, ports.IdempotencyRetention-time.Minute)
	putAnswer(t, f, "live", "v-live")
	if n, err := f.Store.PurgeExpired(ctx, 100); err != nil || n != 1 {
		t.Fatalf("purge = %d %v, want 1 (only the expired row)", n, err)
	}
	wantReplay(t, f, "edge", "v-edge")
	wantReplay(t, f, "live", "v-live")
	if n, err := f.Store.PurgeExpired(ctx, 100); err != nil || n != 0 {
		t.Fatalf("second purge = %d %v, want 0", n, err)
	}
}

// purgeBatches: one call deletes at most limit rows, the next ones finish.
func purgeBatches(t *testing.T, f IdempotencyFixture) {
	for _, k := range []string{"a", "b", "c", "d", "e"} {
		putAnswer(t, f, k, k)
	}
	f.Age(t, ports.IdempotencyRetention+time.Hour)
	for _, want := range []int{2, 2, 1, 0} {
		if n, err := f.Store.PurgeExpired(ctx, 2); err != nil || n != want {
			t.Fatalf("purge = %d %v, want %d", n, err, want)
		}
	}
}

// purgeOldestFirst: with two expired rows and a limit of one, the older goes.
// Ageing everything back by more than both ages makes the survivor live
// again, which is what tells the two apart (an expired row never replays).
func purgeOldestFirst(t *testing.T, f IdempotencyFixture) {
	putAnswer(t, f, "older", "v-older")
	f.Age(t, time.Hour)
	putAnswer(t, f, "newer", "v-newer")
	f.Age(t, ports.IdempotencyRetention+time.Minute)
	if n, err := f.Store.PurgeExpired(ctx, 1); err != nil || n != 1 {
		t.Fatalf("purge = %d %v, want 1", n, err)
	}
	f.Age(t, -(ports.IdempotencyRetention + 2*time.Hour)) // the survivor would be live again, were it still stored
	wantReplay(t, f, "newer", "v-newer")
	if rec, err := f.Store.Lock(ctx, "older"); err != nil || rec != nil {
		t.Fatalf("lock of the purged key = %v %v, want nil", rec, err)
	}
}

// purgeWithoutBudget: a purge with no budget deletes nothing; the rows stay
// for the next pass (a worker must be able to make progress in later calls).
func purgeWithoutBudget(t *testing.T, f IdempotencyFixture) {
	putAnswer(t, f, "a", "v-a")
	putAnswer(t, f, "b", "v-b")
	f.Age(t, ports.IdempotencyRetention+time.Hour)
	if n, err := f.Store.PurgeExpired(ctx, 0); err != nil || n != 0 {
		t.Fatalf("purge with no budget = %d %v, want 0", n, err)
	}
	if n, err := f.Store.PurgeExpired(ctx, 100); err != nil || n != 2 {
		t.Fatalf("the rows are still there: purge = %d %v, want 2", n, err)
	}
}

// expiredNeverReplays pins the read-side age guard both stores share: a row
// past ports.IdempotencyRetention answers nothing, purge or no purge.
func expiredNeverReplays(t *testing.T, f IdempotencyFixture) {
	putAnswer(t, f, "stale", "v-stale")
	f.Age(t, ports.IdempotencyRetention+time.Minute)
	if rec, err := f.Store.Lock(ctx, "stale"); err != nil || rec != nil {
		t.Fatalf("lock of an expired key = %v %v, want nil: the window is enforced at read", rec, err)
	}
}

func putAnswer(t *testing.T, f IdempotencyFixture, key, body string) {
	t.Helper()
	noErr(t, f.Store.Save(ctx, idempotency.Record{Key: key, Fingerprint: "fp", Status: 200, Body: []byte(body)}))
}

func wantReplay(t *testing.T, f IdempotencyFixture, key, body string) {
	t.Helper()
	rec, err := f.Store.Lock(ctx, key)
	if err != nil || rec == nil {
		t.Fatalf("lock %s = %v %v, want the stored answer replayed", key, rec, err)
	}
	if string(rec.Body) != body {
		t.Fatalf("lock %s = %q, want the stored %q", key, rec.Body, body)
	}
}
