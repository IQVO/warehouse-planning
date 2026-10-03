//go:build integration

package postgres_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/claudioed/warehouse-planning/internal/adapters/outbound/postgres"
	"github.com/claudioed/warehouse-planning/internal/adapters/outbound/postgres/pgtx"
	"github.com/claudioed/warehouse-planning/internal/domain/processcapacity"
)

func newMigratedPool(t *testing.T) *pgxpool.Pool {
	t.Helper()
	databaseURL := startPostgres(t)
	if err := postgres.RunMigrations(databaseURL, migrationsDir(t)); err != nil {
		t.Fatalf("migrations: %v", err)
	}
	pool, err := postgres.NewPool(context.Background(), databaseURL)
	if err != nil {
		t.Fatalf("pool: %v", err)
	}
	t.Cleanup(pool.Close)
	return pool
}

type uowRepos struct {
	pool      *pgxpool.Pool
	uow       *postgres.UnitOfWork
	pcs       *postgres.ProcessCapacityRepo
	processed *postgres.ProcessedEventRepo
	tally     *postgres.StorageTallyRepo
}

func newUowRepos(t *testing.T) uowRepos {
	pool := newMigratedPool(t)
	return uowRepos{pool, postgres.NewUnitOfWork(pool), postgres.NewProcessCapacityRepo(pool), postgres.NewProcessedEventRepo(pool), postgres.NewStorageTallyRepo(pool)}
}

// touch writes through all three repos using ctx.
func (r uowRepos) touch(t *testing.T, ctx context.Context) {
	t.Helper()
	if claimed, err := r.processed.Claim(ctx, "c", "e1"); err != nil || !claimed {
		t.Fatalf("Claim = %v, %v", claimed, err)
	}
	if up, err := r.tally.RegisterSlot(ctx, "LOC-1", "Z", "LOCATION", []string{"BULK"}); err != nil || len(up) != 1 {
		t.Fatalf("RegisterSlot = %v, %v", up, err)
	}
	w, _ := processcapacity.NewCapacityWindow(time.Unix(0, 0).UTC(), time.Unix(3600, 0).UTC())
	pc := processcapacity.NewProcessCapacity("PICK", "WH1", w)
	rate, _ := processcapacity.NewCapacityRate(5, processcapacity.UnitUnit, time.Hour)
	if err := pc.AddConstraint(processcapacity.ConstraintLabor, rate); err != nil {
		t.Fatal(err)
	}
	if err := r.pcs.Save(ctx, pc); err != nil {
		t.Fatalf("Save: %v", err)
	}
}

// rowCounts returns (processed_events, location_slot_registration,
// location_slot_tally, process_capacity, process_capacity_constraint).
func (r uowRepos) rowCounts(t *testing.T) [5]int {
	t.Helper()
	var out [5]int
	for i, table := range []string{"processed_events", "location_slot_registration", "location_slot_tally", "process_capacity", "process_capacity_constraint"} {
		if err := r.pool.QueryRow(context.Background(), "SELECT count(*) FROM "+table).Scan(&out[i]); err != nil {
			t.Fatalf("count %s: %v", table, err)
		}
	}
	return out
}

func TestUnitOfWork_Postgres_NilCommitsEveryRepo(t *testing.T) {
	r := newUowRepos(t)
	err := r.uow.Do(context.Background(), func(ctx context.Context) error {
		if _, ok := pgtx.From(ctx); !ok {
			t.Error("fn's ctx carries no transaction")
		}
		r.touch(t, ctx)
		return nil
	})
	if err != nil {
		t.Fatalf("Do: %v", err)
	}
	if got := r.rowCounts(t); got != [5]int{1, 1, 1, 1, 1} {
		t.Fatalf("row counts = %v, want all 1", got)
	}
}

func TestUnitOfWork_Postgres_ErrorRollsBackEveryRepoAndUnclaims(t *testing.T) {
	r := newUowRepos(t)
	boom := errors.New("boom")
	err := r.uow.Do(context.Background(), func(ctx context.Context) error {
		r.touch(t, ctx)
		return boom
	})
	if !errors.Is(err, boom) {
		t.Fatalf("Do err = %v, want boom", err)
	}
	if got := r.rowCounts(t); got != [5]int{} {
		t.Fatalf("row counts after rollback = %v, want all 0", got)
	}
	// The un-claim is real: the same event can be claimed again.
	if claimed, err := r.processed.Claim(context.Background(), "c", "e1"); err != nil || !claimed {
		t.Fatalf("Claim after rollback = %v, %v; want true", claimed, err)
	}
}

func TestUnitOfWork_Postgres_PanicRollsBack(t *testing.T) {
	r := newUowRepos(t)
	func() {
		defer func() {
			if recover() == nil {
				t.Fatal("expected panic to propagate")
			}
		}()
		_ = r.uow.Do(context.Background(), func(ctx context.Context) error {
			r.touch(t, ctx)
			panic("kaboom")
		})
	}()
	if got := r.rowCounts(t); got != [5]int{} {
		t.Fatalf("row counts after panic = %v, want all 0", got)
	}
}

func TestUnitOfWork_Postgres_NestedDoJoinsOuterTransaction(t *testing.T) {
	r := newUowRepos(t)
	boom := errors.New("boom")
	err := r.uow.Do(context.Background(), func(outer context.Context) error {
		outerTx, _ := pgtx.From(outer)
		if err := r.uow.Do(outer, func(inner context.Context) error {
			innerTx, ok := pgtx.From(inner)
			if !ok || innerTx != outerTx {
				t.Error("nested Do began its own transaction instead of joining")
			}
			r.touch(t, inner)
			return nil
		}); err != nil {
			return err
		}
		return boom // outer fails AFTER the inner "succeeded"
	})
	if !errors.Is(err, boom) {
		t.Fatalf("Do err = %v", err)
	}
	if got := r.rowCounts(t); got != [5]int{} {
		t.Fatalf("inner work survived an outer rollback: %v", got)
	}
}

// Without a UnitOfWork in ctx the repos behave exactly as before: each call
// commits on its own (REST paths unchanged).
func TestRepos_WithoutUnitOfWork_CommitOnTheirOwn(t *testing.T) {
	r := newUowRepos(t)
	r.touch(t, context.Background())
	if got := r.rowCounts(t); got != [5]int{1, 1, 1, 1, 1} {
		t.Fatalf("row counts = %v, want all 1", got)
	}
	// Decommission standalone also commits.
	if up, found, err := r.tally.DecommissionSlot(context.Background(), "LOC-1"); err != nil || !found || up[0].Count != 0 {
		t.Fatalf("DecommissionSlot = %v %v %v", up, found, err)
	}
	if got := r.rowCounts(t); got[1] != 0 {
		t.Fatalf("registration row not deleted: %v", got)
	}
}

// A failing statement INSIDE a unit of work surfaces from Do and nothing
// the earlier statements wrote survives (a real Postgres error, not a
// wrapper-injected one).
func TestUnitOfWork_Postgres_RealStatementErrorRollsBackEarlierWrites(t *testing.T) {
	r := newUowRepos(t)
	err := r.uow.Do(context.Background(), func(ctx context.Context) error {
		if _, err := r.processed.Claim(ctx, "c", "e1"); err != nil {
			return err
		}
		tx, _ := pgtx.From(ctx)
		_, err := tx.Exec(ctx, "INSERT INTO table_that_does_not_exist VALUES (1)")
		return err
	})
	if err == nil {
		t.Fatal("expected the undefined-table error")
	}
	if got := r.rowCounts(t); got[0] != 0 {
		t.Fatalf("claim survived a failed unit of work: %v", got)
	}
}
