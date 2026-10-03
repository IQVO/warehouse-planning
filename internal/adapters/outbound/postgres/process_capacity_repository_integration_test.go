//go:build integration

package postgres_test

import (
	"context"
	"path/filepath"
	"runtime"
	"testing"
	"time"

	"github.com/testcontainers/testcontainers-go"
	tcpostgres "github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/wait"

	"github.com/claudioed/warehouse-planning/internal/adapters/outbound/postgres"
	"github.com/claudioed/warehouse-planning/internal/domain/processcapacity"
)

// migrationsDir resolves this package's migrations/ directory relative to
// this test file, so the test works regardless of the working directory
// `go test` is invoked from.
func migrationsDir(t *testing.T) string {
	t.Helper()
	_, thisFile, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("unable to resolve test file path")
	}
	return filepath.Join(filepath.Dir(thisFile), "migrations")
}

// startPostgres boots a real Postgres via testcontainers-go (never a
// skip-gated external DB -- see HARNESS.md's TestKafkaIntegrationTestsUseTestcontainers
// sibling rule; this fleet's integration job otherwise provisions nothing
// a Postgres-touching test could silently rely on) and returns a
// password-less-looking connection string (the container's randomly
// generated password is injected by the testcontainers module itself, not
// embedded by this test -- see HARNESS.md's credential-handling note).
func startPostgres(t *testing.T) string {
	t.Helper()
	ctx := context.Background()

	container, err := tcpostgres.Run(ctx,
		"postgres:16-alpine",
		tcpostgres.WithDatabase("warehouse_planning_test"),
		tcpostgres.WithUsername("warehouse_planning_test"),
		tcpostgres.WithPassword("warehouse_planning_test"),
		testcontainers.WithWaitStrategy(
			wait.ForLog("database system is ready to accept connections").WithOccurrence(2).WithStartupTimeout(60*time.Second),
		),
	)
	if err != nil {
		t.Fatalf("unexpected error starting postgres container: %v", err)
	}
	t.Cleanup(func() {
		if err := container.Terminate(context.Background()); err != nil {
			t.Logf("failed to terminate postgres container: %v", err)
		}
	})

	connStr, err := container.ConnectionString(ctx, "sslmode=disable")
	if err != nil {
		t.Fatalf("unexpected error resolving connection string: %v", err)
	}
	return connStr
}

// TestProcessCapacityRepo_SaveLoadRoundTrip proves a real save+load
// against a real Postgres reproduces the PICK/PICK-ZONE-A worked example's
// effective rate and binding constraint.
func TestProcessCapacityRepo_SaveLoadRoundTrip(t *testing.T) {
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

	repo := postgres.NewProcessCapacityRepo(pool)

	start := time.Date(2026, 10, 5, 8, 0, 0, 0, time.UTC)
	end := time.Date(2026, 10, 5, 9, 0, 0, 0, time.UTC)
	window, err := processcapacity.NewCapacityWindow(start, end)
	if err != nil {
		t.Fatalf("unexpected error building window: %v", err)
	}

	pc := processcapacity.NewProcessCapacity("PICK", "PICK-ZONE-A", window)
	registrations := []struct {
		constraintType processcapacity.ConstraintType
		quantity       float64
	}{
		{processcapacity.ConstraintLabor, 4000},
		{processcapacity.ConstraintLocation, 3500},
		{processcapacity.ConstraintEquipment, 5000},
		{processcapacity.ConstraintConveyor, 3800},
	}
	for _, reg := range registrations {
		rate, err := processcapacity.NewCapacityRate(reg.quantity, processcapacity.UnitUnit, time.Hour)
		if err != nil {
			t.Fatalf("unexpected error building rate: %v", err)
		}
		if err := pc.AddConstraint(reg.constraintType, rate); err != nil {
			t.Fatalf("unexpected error adding constraint %v: %v", reg.constraintType, err)
		}
	}

	if err := repo.Save(ctx, pc); err != nil {
		t.Fatalf("unexpected error saving: %v", err)
	}

	loaded, err := repo.FindByProcessLocationWindow(ctx, "PICK", "PICK-ZONE-A", start, end)
	if err != nil {
		t.Fatalf("unexpected error loading: %v", err)
	}
	if loaded == nil {
		t.Fatal("expected to find the saved ProcessCapacity")
	}

	effective, binding, err := loaded.EffectiveRate()
	if err != nil {
		t.Fatalf("unexpected error computing effective rate: %v", err)
	}
	if binding != processcapacity.ConstraintLocation {
		t.Fatalf("expected LOCATION to be binding after round trip, got %v", binding)
	}
	if effective.Quantity() != 3500 || effective.Unit() != processcapacity.UnitUnit || effective.Period() != time.Hour {
		t.Fatalf("expected 3500 UNIT/HOUR after round trip, got %v %v/%v", effective.Quantity(), effective.Unit(), effective.Period())
	}
	if got := len(loaded.Constraints()); got != 4 {
		t.Fatalf("expected 4 constraints after round trip, got %d", got)
	}
}

// TestProcessCapacityRepo_FindByProcessLocationWindow_NotFound proves the
// nil,nil contract for an identity nothing has ever been saved under.
func TestProcessCapacityRepo_FindByProcessLocationWindow_NotFound(t *testing.T) {
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

	repo := postgres.NewProcessCapacityRepo(pool)
	start := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	end := time.Date(2026, 1, 1, 1, 0, 0, 0, time.UTC)

	found, err := repo.FindByProcessLocationWindow(ctx, "PACK", "NOWHERE", start, end)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if found != nil {
		t.Fatalf("expected nil for an unregistered identity, got %+v", found)
	}
}
