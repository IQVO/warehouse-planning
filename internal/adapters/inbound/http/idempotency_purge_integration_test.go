//go:build integration

package http_test

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	inboundhttp "github.com/claudioed/warehouse-planning/internal/adapters/inbound/http"
	"github.com/claudioed/warehouse-planning/internal/adapters/outbound/postgres"
	"github.com/claudioed/warehouse-planning/internal/application/ports"
)

// ADR 0026 addendum (2026-11-23, retention): nothing used to delete an
// expired idempotency_keys row -- the 24h window was enforced at read/upsert
// only, so the table grew forever. These proofs pin the purge against a real
// Postgres, next to the real middleware: only expired rows go, in bounded
// batches, never a row a request holds, and an expired key EXECUTES instead
// of answering. Red until the purge lands.

// purgeServer wires the REAL UnitOfWork + IdempotencyStore over a private
// migrated Postgres (the same shape the poison proofs use), plus a probe
// table the wrapped handler writes through the request's unit of work.
func purgeServer(t *testing.T) (*inboundhttp.Server, *pgxpool.Pool, *atomic.Int32, *atomic.Pointer[error]) {
	t.Helper()
	pool, err := postgres.NewPool(context.Background(), httpMigratedDB(t))
	if err != nil {
		t.Fatalf("open pool: %v", err)
	}
	t.Cleanup(pool.Close)
	if _, err := pool.Exec(context.Background(), `CREATE TABLE IF NOT EXISTS purge_probe (tag text NOT NULL)`); err != nil {
		t.Fatalf("create purge_probe: %v", err)
	}
	return &inboundhttp.Server{
		UnitOfWork:  postgres.NewUnitOfWork(pool),
		Idempotency: postgres.NewIdempotencyStore(pool),
	}, pool, new(atomic.Int32), new(atomic.Pointer[error])
}

// purgeHandler answers 200 and writes one probe row through the request's
// unit of work, counting how many times the key EXECUTED. A write error is
// captured for the test's failure message (it becomes a 500 the middleware
// does not store).
func purgeHandler(calls *atomic.Int32, writeErr *atomic.Pointer[error]) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		if err := inboundhttp.ExportWriteInRequestUnit(r.Context(), `INSERT INTO purge_probe (tag) VALUES ($1)`, r.Header.Get(inboundhttp.IdempotencyKeyHeader)); err != nil {
			writeErr.CompareAndSwap(nil, &err)
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		// A stored answer's body column is BYTEA NOT NULL: the answer must
		// carry bytes (a nil []byte would encode as NULL and fail the save).
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	}
}

// seedKeyRow stores a key row that was created age ago.
func seedKeyRow(t *testing.T, pool *pgxpool.Pool, key string, age time.Duration) {
	t.Helper()
	if _, err := pool.Exec(context.Background(), `INSERT INTO idempotency_keys (key, fingerprint, status, body, created_at)
		VALUES ($1, 'fp', 200, '\x7b7d'::bytea, now() - $2::interval)`, key, age.String()); err != nil {
		t.Fatalf("seed %s: %v", key, err)
	}
}

// purgeKeys lists the keys still stored, in order.
func purgeKeys(t *testing.T, pool *pgxpool.Pool) []string {
	t.Helper()
	rows, err := pool.Query(context.Background(), `SELECT key FROM idempotency_keys ORDER BY key`)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	var out []string
	for rows.Next() {
		var k string
		if err := rows.Scan(&k); err != nil {
			t.Fatal(err)
		}
		out = append(out, k)
	}
	return out
}

func TestPurgeDeletesExpiredKeysAndKeepsLiveOnes(t *testing.T) {
	_, pool, _, _ := purgeServer(t)
	seedKeyRow(t, pool, "expired-a", ports.IdempotencyRetention+time.Hour)
	seedKeyRow(t, pool, "expired-b", 48*time.Hour)
	seedKeyRow(t, pool, "live-a", time.Hour)
	seedKeyRow(t, pool, "live-b", ports.IdempotencyRetention-time.Hour)

	n, err := postgres.NewIdempotencyStore(pool).PurgeExpired(context.Background(), 100)
	if err != nil || n != 2 {
		t.Fatalf("purge = %d, %v, want 2 expired rows deleted", n, err)
	}
	if got := purgeKeys(t, pool); !slices.Equal(got, []string{"live-a", "live-b"}) {
		t.Fatalf("rows left = %v, want only the live ones", got)
	}
	if n, err := postgres.NewIdempotencyStore(pool).PurgeExpired(context.Background(), 100); err != nil || n != 0 {
		t.Fatalf("a second purge = %d, %v, want 0: nothing live is ever deleted", n, err)
	}
}

func TestPurgeDeletesAtMostOneBatchPerCall(t *testing.T) {
	_, pool, _, _ := purgeServer(t)
	for i := range 5 {
		seedKeyRow(t, pool, fmt.Sprintf("old-%d", i), 30*time.Hour)
	}
	seedKeyRow(t, pool, "live", time.Minute)
	store := postgres.NewIdempotencyStore(pool)

	if n, err := store.PurgeExpired(context.Background(), 2); err != nil || n != 2 {
		t.Fatalf("first batch = %d, %v, want 2", n, err)
	}
	var left int
	if err := pool.QueryRow(context.Background(), `SELECT count(*) FROM idempotency_keys`).Scan(&left); err != nil {
		t.Fatal(err)
	}
	if left != 4 {
		t.Fatalf("rows after one batch of 2 = %d, want 4 (3 expired + 1 live)", left)
	}
	total := 2
	for {
		n, err := store.PurgeExpired(context.Background(), 2)
		if err != nil {
			t.Fatal(err)
		}
		if n == 0 {
			break
		}
		total += n
	}
	if got := purgeKeys(t, pool); total != 5 || !slices.Equal(got, []string{"live"}) {
		t.Fatalf("purged %d rows, left %v, want 5 and only the live key", total, got)
	}
}

// The age guard, through the real middleware: a key whose stored row is past
// the 24h window EXECUTES again instead of answering the stored 200.
func TestAnExpiredKeyExecutesInsteadOfReplaying(t *testing.T) {
	s, pool, calls, writeErr := purgeServer(t)
	h := s.ExportIdempotent(purgeHandler(calls, writeErr))
	post := func() *httptest.ResponseRecorder {
		req := httptest.NewRequest(http.MethodPost, "/capacity-plans", nil)
		req.Header.Set(inboundhttp.IdempotencyKeyHeader, "age-guard")
		rec := httptest.NewRecorder()
		h(rec, req)
		return rec
	}

	if r := post(); r.Code != http.StatusOK || r.Header().Get(inboundhttp.IdempotencyReplayedHeader) == "true" || calls.Load() != 1 {
		we := "none"
		if e := writeErr.Load(); e != nil {
			we = (*e).Error()
		}
		t.Fatalf("first post = %d replayed %v calls %d body %q writeErr %v, want a fresh 200", r.Code, r.Header().Get(inboundhttp.IdempotencyReplayedHeader), calls.Load(), r.Body.String(), we)
	}
	if _, err := pool.Exec(context.Background(), `UPDATE idempotency_keys SET created_at = now() - $1::interval WHERE key = 'age-guard'`, (ports.IdempotencyRetention + time.Hour).String()); err != nil {
		t.Fatalf("age the stored row: %v", err)
	}
	if r := post(); r.Code != http.StatusOK || r.Header().Get(inboundhttp.IdempotencyReplayedHeader) == "true" || calls.Load() != 2 {
		t.Fatalf("post after expiry = %d replayed %v calls %d, want a fresh EXECUTION, not the stored answer", r.Code, r.Header().Get(inboundhttp.IdempotencyReplayedHeader), calls.Load())
	}
	if r := post(); r.Header().Get(inboundhttp.IdempotencyReplayedHeader) != "true" || calls.Load() != 2 {
		t.Fatalf("a repeat of the refreshed key replays: header %q calls %d", r.Header().Get(inboundhttp.IdempotencyReplayedHeader), calls.Load())
	}
}

// A request holds the key (advisory lock) and, last, refreshes its row. The
// purge must neither delete the row the request is about to refresh nor wait
// for the request: FOR UPDATE SKIP LOCKED skips the locked row.
func TestPurgeNeverDeletesARowARequestHoldsAndNeverWaitsForIt(t *testing.T) {
	_, pool, _, _ := purgeServer(t)
	ctx := context.Background()
	seedKeyRow(t, pool, "held", 30*time.Hour)

	tx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	// What Lock does: the transaction-scoped advisory lock on the key...
	if _, err := tx.Exec(ctx, `SELECT pg_advisory_xact_lock(hashtextextended($1, 0))`, "held"); err != nil {
		t.Fatal(err)
	}
	// ...and what Save does: refresh the (expired) row; the row lock lives
	// until the commit.
	if _, err := tx.Exec(ctx, `INSERT INTO idempotency_keys (key, fingerprint, status, body) VALUES ($1, 'fp2', 200, '\x7b7d'::bytea)
		ON CONFLICT (key) DO UPDATE SET fingerprint = 'fp2', created_at = now()`, "held"); err != nil {
		t.Fatal(err)
	}

	purgeCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	if n, err := postgres.NewIdempotencyStore(pool).PurgeExpired(purgeCtx, 100); err != nil || n != 0 {
		t.Fatalf("purge while the request holds the row = %d, %v, want 0 and no wait", n, err)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	if n, err := postgres.NewIdempotencyStore(pool).PurgeExpired(ctx, 100); err != nil || n != 0 {
		t.Fatalf("purge after the request committed = %d, %v: the row was refreshed, it is live", n, err)
	}
	if got := purgeKeys(t, pool); !slices.Equal(got, []string{"held"}) {
		t.Fatalf("rows = %v, want the refreshed key", got)
	}
}

// The real middleware on a key whose row has expired, while the purge runs at
// the same time: every request is a FRESH one (an expired row never replays),
// answers 200, and leaves exactly one live row per key.
func TestPurgeRacingARequestOnAnExpiredKeyKeepsTheRequestCorrect(t *testing.T) {
	s, pool, calls, writeErr := purgeServer(t)
	h := s.ExportIdempotent(purgeHandler(calls, writeErr))
	store := postgres.NewIdempotencyStore(pool)
	post := func(key string) int {
		req := httptest.NewRequest(http.MethodPost, "/capacity-plans", nil)
		req.Header.Set(inboundhttp.IdempotencyKeyHeader, key)
		rec := httptest.NewRecorder()
		h(rec, req)
		return rec.Code
	}

	const rounds = 25
	for i := range rounds {
		key := fmt.Sprintf("race-%d", i)
		seedKeyRow(t, pool, key, 30*time.Hour)
		var wg sync.WaitGroup
		codes := make([]int, 3)
		for j := range codes {
			wg.Add(1)
			go func() { defer wg.Done(); codes[j] = post(key) }()
		}
		wg.Add(1)
		go func() { defer wg.Done(); _, _ = store.PurgeExpired(context.Background(), 100) }()
		wg.Wait()
		for _, c := range codes {
			if c != http.StatusOK {
				we := "none"
				if e := writeErr.Load(); e != nil {
					we = (*e).Error()
				}
				t.Fatalf("round %d: a request racing the purge answered %d, want 200 (writeErr %v)", i, c, we)
			}
		}
	}
	if calls.Load() != rounds {
		t.Fatalf("the handler ran %d times for %d keys: an expired row must be a fresh request, once, and its duplicates replay", calls.Load(), rounds)
	}
	var left int
	if err := pool.QueryRow(context.Background(), `SELECT count(*) FROM idempotency_keys WHERE key LIKE 'race-%'`).Scan(&left); err != nil {
		t.Fatal(err)
	}
	if left != rounds {
		t.Fatalf("%d key rows left, want one live row per key (%d)", left, rounds)
	}
	req := httptest.NewRequest(http.MethodPost, "/capacity-plans", nil)
	req.Header.Set(inboundhttp.IdempotencyKeyHeader, "race-0")
	rec := httptest.NewRecorder()
	h(rec, req)
	if rec.Header().Get(inboundhttp.IdempotencyReplayedHeader) != "true" {
		t.Fatalf("a repeat of a stored key replays: %v", rec.Header())
	}
}
