//go:build integration

package http_test

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"

	inboundhttp "github.com/claudioed/warehouse-planning/internal/adapters/inbound/http"
	"github.com/claudioed/warehouse-planning/internal/adapters/outbound/postgres"
	"github.com/claudioed/warehouse-planning/internal/adapters/outbound/postgres/pgtx"
)

// poisonServer wires the REAL UnitOfWork + IdempotencyStore over a private
// migrated Postgres, so the middleware runs exactly as cmd/api wires it.
func poisonServer(t *testing.T) (*inboundhttp.Server, *pgxpool.Pool) {
	t.Helper()
	pool, err := postgres.NewPool(context.Background(), httpMigratedDB(t))
	if err != nil {
		t.Fatalf("open pool: %v", err)
	}
	t.Cleanup(pool.Close)
	return &inboundhttp.Server{
		UnitOfWork:  postgres.NewUnitOfWork(pool),
		Idempotency: postgres.NewIdempotencyStore(pool),
	}, pool
}

// poisonTx simulates what a mapped domain conflict does to the shared
// transaction: a statement fails on the server (a unique violation, like a
// lost race on a unique index), the repository maps it to a typed error and
// the handler answers 4xx -- but the transaction is ABORTED from then on.
func poisonTx(t *testing.T, r *http.Request) {
	t.Helper()
	tx, ok := pgtx.From(r.Context())
	if !ok {
		t.Fatal("the unit of work did not bind a transaction into the request context")
	}
	for i := 0; i < 2; i++ {
		if _, err := tx.Exec(r.Context(), `INSERT INTO idempotency_keys (key, fingerprint, status, body) VALUES ('dup', 'f', 200, ''::bytea)`); err != nil {
			return // the second insert aborts the transaction (23505)
		}
	}
	t.Fatal("the duplicate insert did not abort the transaction")
}

func poisonRequest(key string) *http.Request {
	req := httptest.NewRequest(http.MethodPost, "/capacity-plans/plan-x/publications", strings.NewReader(`{}`))
	req.Header.Set(inboundhttp.IdempotencyKeyHeader, key)
	return req
}

func countRows(t *testing.T, pool *pgxpool.Pool, key string) int {
	t.Helper()
	var n int
	if err := pool.QueryRow(context.Background(), `SELECT count(*) FROM idempotency_keys WHERE key = $1`, key).Scan(&n); err != nil {
		t.Fatal(err)
	}
	return n
}

// TestIdempotency_AbortedTransaction409CommitsTheOutcome: a lost race that
// aborts the transaction still answers the truthful 409, the outcome is
// committed, and a replay returns it without re-running the handler.
func TestIdempotency_AbortedTransaction409CommitsTheOutcome(t *testing.T) {
	s, pool := poisonServer(t)
	var calls atomic.Int64
	mw := s.ExportIdempotent(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		poisonTx(t, r)
		w.Header().Set("Content-Type", "application/problem+json")
		w.WriteHeader(http.StatusConflict)
		_, _ = w.Write([]byte(`{"type":"https://errors.example/some-conflict","status":409}`))
	})

	first := httptest.NewRecorder()
	mw(first, poisonRequest("k-poison"))
	if first.Code != http.StatusConflict {
		t.Fatalf("first = %d %s, want the truthful 409", first.Code, first.Body.String())
	}
	var status int
	if err := pool.QueryRow(context.Background(), `SELECT status FROM idempotency_keys WHERE key = 'k-poison'`).Scan(&status); err != nil || status != http.StatusConflict {
		t.Fatalf("409 outcome not committed: status=%d err=%v", status, err)
	}
	if n := countRows(t, pool, "dup"); n != 0 {
		t.Fatalf("handler's partial writes survived: %d", n)
	}

	replay := httptest.NewRecorder()
	mw(replay, poisonRequest("k-poison"))
	if replay.Code != http.StatusConflict || replay.Header().Get(inboundhttp.IdempotencyReplayedHeader) != "true" {
		t.Fatalf("replay = %d replayed=%q, want the cached 409", replay.Code, replay.Header().Get(inboundhttp.IdempotencyReplayedHeader))
	}
	if replay.Body.String() != first.Body.String() {
		t.Fatalf("replay body = %s, want %s", replay.Body.String(), first.Body.String())
	}
	if calls.Load() != 1 {
		t.Fatalf("handler ran %d times, want 1 (replay must not re-run it)", calls.Load())
	}
}

// TestIdempotency_AbortedTransaction2xxIsNeverCached: a success whose writes
// died with the transaction is a lie; it fails with 500, caches nothing, and
// the same key retries the real work.
func TestIdempotency_AbortedTransaction2xxIsNeverCached(t *testing.T) {
	s, pool := poisonServer(t)
	var calls atomic.Int64
	mw := s.ExportIdempotent(func(w http.ResponseWriter, r *http.Request) {
		if calls.Add(1) == 1 {
			poisonTx(t, r) // only the first attempt dies inside the transaction
		}
		w.WriteHeader(http.StatusCreated)
		_, _ = w.Write([]byte(`{"planId":"p1"}`))
	})

	first := httptest.NewRecorder()
	mw(first, poisonRequest("k-lie"))
	if first.Code != http.StatusInternalServerError {
		t.Fatalf("first = %d %s, want 500 (the writes never happened)", first.Code, first.Body.String())
	}
	if n := countRows(t, pool, "k-lie"); n != 0 {
		t.Fatalf("a failed success left %d idempotency rows: the key could not retry", n)
	}

	second := httptest.NewRecorder()
	mw(second, poisonRequest("k-lie"))
	if second.Code != http.StatusCreated {
		t.Fatalf("retry = %d %s, want the real work to succeed", second.Code, second.Body.String())
	}
	if calls.Load() != 2 {
		t.Fatalf("handler ran %d times, want 2", calls.Load())
	}
}
