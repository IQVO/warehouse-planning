//go:build integration

package postgres_test

import (
	"context"
	"errors"
	"reflect"
	"testing"

	"github.com/claudioed/warehouse-planning/internal/adapters/outbound/postgres"
	"github.com/claudioed/warehouse-planning/internal/domain/processpath"
)

func mustPath(t *testing.T, id, name string, steps ...processpath.ProcessType) processpath.ProcessPath {
	t.Helper()
	p, err := processpath.NewProcessPath(id, name, steps)
	if err != nil {
		t.Fatalf("unexpected error building ProcessPath: %v", err)
	}
	return p
}

// TestProcessPathRepo_SaveLoadPreservesStepOrderAcrossReinstantiation proves
// a save+load round trip against a real Postgres keeps the steps in their
// declared order (deliberately NOT alphabetical, with a repeated step) and
// that the data survives a brand new repository/pool -- the pod-restart
// scenario this repository exists for.
func TestProcessPathRepo_SaveLoadPreservesStepOrderAcrossReinstantiation(t *testing.T) {
	databaseURL := startPostgres(t)
	if err := postgres.RunMigrations(databaseURL, migrationsDir(t)); err != nil {
		t.Fatalf("unexpected error running migrations: %v", err)
	}
	ctx := context.Background()

	pool1, err := postgres.NewPool(ctx, databaseURL)
	if err != nil {
		t.Fatalf("unexpected error opening pool: %v", err)
	}
	steps := []processpath.ProcessType{"PICK", "REBIN", "PACK", "PICK", "AUDIT"}
	if err := postgres.NewProcessPathRepo(pool1).Save(ctx, mustPath(t, "pick-rebin-pack", "Pick-Rebin-Pack", steps...)); err != nil {
		t.Fatalf("unexpected error saving: %v", err)
	}
	pool1.Close()

	// "Restart": a fresh pool and a fresh repository over the same database.
	pool2, err := postgres.NewPool(ctx, databaseURL)
	if err != nil {
		t.Fatalf("unexpected error reopening pool: %v", err)
	}
	defer pool2.Close()

	loaded, err := postgres.NewProcessPathRepo(pool2).FindByID(ctx, "pick-rebin-pack")
	if err != nil {
		t.Fatalf("unexpected error loading: %v", err)
	}
	if loaded == nil {
		t.Fatal("expected the saved ProcessPath to survive repository re-instantiation")
	}
	if loaded.ID() != "pick-rebin-pack" || loaded.Name() != "Pick-Rebin-Pack" {
		t.Fatalf("unexpected id/name after round trip: %q / %q", loaded.ID(), loaded.Name())
	}
	if !reflect.DeepEqual(loaded.Steps(), steps) {
		t.Fatalf("step order not preserved: want %v, got %v", steps, loaded.Steps())
	}
}

// TestProcessPathRepo_SaveSameIDReplacesWholesaleAndNotFoundIsNil mirrors
// the in-memory repository's semantics: saving an existing id replaces name
// and steps (upsert, no error), and an unknown id is (nil, nil).
func TestProcessPathRepo_SaveSameIDReplacesWholesaleAndNotFoundIsNil(t *testing.T) {
	databaseURL := startPostgres(t)
	if err := postgres.RunMigrations(databaseURL, migrationsDir(t)); err != nil {
		t.Fatalf("unexpected error running migrations: %v", err)
	}
	ctx := context.Background()
	pool, err := postgres.NewPool(ctx, databaseURL)
	if err != nil {
		t.Fatalf("unexpected error opening pool: %v", err)
	}
	defer pool.Close()
	repo := postgres.NewProcessPathRepo(pool)

	missing, err := repo.FindByID(ctx, "nope")
	if err != nil || missing != nil {
		t.Fatalf("expected (nil, nil) for an unknown id, got (%v, %v)", missing, err)
	}

	if err := repo.Save(ctx, mustPath(t, "p1", "First", "PICK", "PACK")); err != nil {
		t.Fatalf("unexpected error on first save: %v", err)
	}
	if err := repo.Save(ctx, mustPath(t, "p1", "Second", "PACK", "REBIN", "PICK")); err != nil {
		t.Fatalf("expected re-saving the same id to upsert, got: %v", err)
	}
	loaded, err := repo.FindByID(ctx, "p1")
	if err != nil || loaded == nil {
		t.Fatalf("unexpected load result (%v, %v)", loaded, err)
	}
	if loaded.Name() != "Second" || !reflect.DeepEqual(loaded.Steps(), []processpath.ProcessType{"PACK", "REBIN", "PICK"}) {
		t.Fatalf("expected wholesale replacement, got name=%q steps=%v", loaded.Name(), loaded.Steps())
	}
}

// TestProcessPathRepo_JoinsUnitOfWorkTransaction proves Save participates in
// the ctx-carried transaction: rolled back with the unit of work when fn
// fails, visible after commit otherwise.
func TestProcessPathRepo_JoinsUnitOfWorkTransaction(t *testing.T) {
	databaseURL := startPostgres(t)
	if err := postgres.RunMigrations(databaseURL, migrationsDir(t)); err != nil {
		t.Fatalf("unexpected error running migrations: %v", err)
	}
	ctx := context.Background()
	pool, err := postgres.NewPool(ctx, databaseURL)
	if err != nil {
		t.Fatalf("unexpected error opening pool: %v", err)
	}
	defer pool.Close()
	repo := postgres.NewProcessPathRepo(pool)
	uow := postgres.NewUnitOfWork(pool)

	boom := errors.New("boom")
	err = uow.Do(ctx, func(ctx context.Context) error {
		if err := repo.Save(ctx, mustPath(t, "rolled-back", "RB", "PICK")); err != nil {
			return err
		}
		inTx, err := repo.FindByID(ctx, "rolled-back")
		if err != nil || inTx == nil {
			t.Fatalf("expected the path to be visible inside the unit of work, got (%v, %v)", inTx, err)
		}
		return boom
	})
	if !errors.Is(err, boom) {
		t.Fatalf("expected boom from Do, got %v", err)
	}
	if got, err := repo.FindByID(ctx, "rolled-back"); err != nil || got != nil {
		t.Fatalf("expected the rolled-back save to be invisible, got (%v, %v)", got, err)
	}

	if err := uow.Do(ctx, func(ctx context.Context) error {
		return repo.Save(ctx, mustPath(t, "committed", "C", "PICK", "PACK"))
	}); err != nil {
		t.Fatalf("unexpected error committing: %v", err)
	}
	if got, err := repo.FindByID(ctx, "committed"); err != nil || got == nil {
		t.Fatalf("expected the committed save to be visible, got (%v, %v)", got, err)
	}
}
