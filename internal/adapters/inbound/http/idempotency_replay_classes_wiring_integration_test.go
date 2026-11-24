//go:build integration

// Rendered from warehouse-harness-template templates/idempotency/replay_classes_wiring_test.go.tmpl
// (origin/develop): the ONLY file of the replay-class pair that knows the store. Over a real
// Postgres (the package's shared testcontainers container, one private database per call), with
// the production Server.idempotent middleware, the production problem writer and writes through
// the request's unit of work (pgtx), so no driver package is named in this file.
package http_test

import (
	"context"
	"net/http"
	"testing"

	inboundhttp "github.com/claudioed/warehouse-planning/internal/adapters/inbound/http"
	"github.com/claudioed/warehouse-planning/internal/adapters/outbound/postgres"
)

// newReplayOps returns the six operations over ONE clean store: a fresh private
// database cloned from the package's migrated template, so every key row and
// every replay_probe write starts at zero.
func newReplayOps(t *testing.T) replayOps {
	t.Helper()
	pool, err := postgres.NewPool(context.Background(), httpMigratedDB(t))
	if err != nil {
		t.Fatalf("open pool: %v", err)
	}
	t.Cleanup(pool.Close)
	if _, err := pool.Exec(context.Background(), `CREATE TABLE IF NOT EXISTS replay_probe (tag text NOT NULL)`); err != nil {
		t.Fatalf("create replay_probe: %v", err)
	}
	uow := postgres.NewUnitOfWork(pool)
	idem := postgres.NewIdempotencyStore(pool)
	count := func(t *testing.T, query string, args ...any) int {
		t.Helper()
		var n int
		if err := pool.QueryRow(context.Background(), query, args...).Scan(&n); err != nil {
			t.Fatal(err)
		}
		return n
	}
	return replayOps{
		Catalogue: func() []problemEntry {
			types := inboundhttp.ExportProblemTypesForTest()
			out := make([]problemEntry, 0, len(types))
			for _, p := range types {
				out = append(out, problemEntry{Slug: p.Slug, Status: p.Status, Class: p.Class})
			}
			return out
		},
		Middleware: func(_ *testing.T, next http.Handler) http.Handler {
			s := &inboundhttp.Server{UnitOfWork: uow, Idempotency: idem}
			return s.ExportIdempotent(next.ServeHTTP)
		},
		Refuse: func(w http.ResponseWriter, r *http.Request, e problemEntry) {
			inboundhttp.ExportRefuseForTest(w, r, e.Slug)
		},
		Write: func(ctx context.Context, tag string) error {
			return inboundhttp.ExportWriteInRequestUnit(ctx, `INSERT INTO replay_probe (tag) VALUES ($1)`, tag)
		},
		Committed: func(t *testing.T, tag string) int {
			return count(t, `SELECT count(*) FROM replay_probe WHERE tag = $1`, tag)
		},
		KeyRows: func(t *testing.T, key string) int {
			return count(t, `SELECT count(*) FROM idempotency_keys WHERE key = $1`, key)
		},
	}
}
