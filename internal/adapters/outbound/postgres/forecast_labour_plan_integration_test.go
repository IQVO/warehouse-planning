//go:build integration

package postgres_test

import (
	"context"
	"errors"
	"testing"

	"github.com/golang-migrate/migrate/v4"

	"github.com/claudioed/warehouse-planning/internal/adapters/outbound/postgres"
	"github.com/claudioed/warehouse-planning/internal/adapters/outbound/repocontract"
	"github.com/claudioed/warehouse-planning/internal/application/ports"
	"github.com/claudioed/warehouse-planning/internal/domain/labourplan"
	"github.com/claudioed/warehouse-planning/internal/domain/workloadforecast"
)

var (
	_ ports.WorkloadForecastRepository = (*postgres.WorkloadForecastRepo)(nil)
	_ ports.LabourPlanRepository       = (*postgres.LabourPlanRepo)(nil)
	_ ports.CommittedHeadsRepository   = (*postgres.CommittedHeadsRepo)(nil)
)

// The Postgres repositories show the behaviour the in-memory doubles show
// (memory/forecast_labour_plan_repositories_test.go runs the same suite):
// round trip, the version guard (duplicate first save and stale update are
// ports.ErrVersionConflict), the List keyset paging, the committed heads of
// the labour plan drift and the unit of work. One database serves the suite;
// every case starts from empty tables.
func TestForecastAndLabourPlanRepos_SatisfyTheRepositoryContract(t *testing.T) {
	pool := newMigratedPool(t)
	repocontract.Run(t, func(t *testing.T) repocontract.Fixture {
		t.Helper()
		if _, err := pool.Exec(context.Background(), `TRUNCATE workload_forecasts, labour_plans, labour_plan_commitments`); err != nil {
			t.Fatal(err)
		}
		return repocontract.Fixture{
			UoW:         postgres.NewUnitOfWork(pool),
			Forecasts:   postgres.NewWorkloadForecastRepo(pool),
			Plans:       postgres.NewLabourPlanRepo(pool),
			Commitments: postgres.NewCommittedHeadsRepo(pool),
		}
	})
}

// A row the CHECKs let through but the domain refuses reads back as an
// error (wrapping the domain's), never as a silently repaired aggregate.
func TestForecastAndLabourPlanRepos_ACorruptRowIsAnError(t *testing.T) {
	pool := newMigratedPool(t)
	ctx := context.Background()
	for _, q := range []string{
		`INSERT INTO workload_forecasts VALUES ('SIM1', 'negative', '[{"cpt":"2026-11-18T08:00:00Z","quantity":-1}]', now(), 1)`,
		`INSERT INTO workload_forecasts VALUES ('SIM1', 'no-quantity', '[{"cpt":"2026-11-18T08:00:00Z"}]', now(), 1)`,
		`INSERT INTO labour_plans VALUES ('SIM1', 'day', 'overflow', 10, 1e308, 10, 10, NULL, 'request', 1)`,
	} {
		if _, err := pool.Exec(ctx, q); err != nil {
			t.Fatalf("%s: %v", q, err)
		}
	}
	forecasts, plans := postgres.NewWorkloadForecastRepo(pool), postgres.NewLabourPlanRepo(pool)
	if f, found, err := forecasts.Get(ctx, "SIM1", "negative"); !errors.Is(err, workloadforecast.ErrNegativeQuantity) || found || f != nil {
		t.Fatalf("Get(negative) = %v, %v, %v; want ErrNegativeQuantity", f, found, err)
	}
	if f, found, err := forecasts.Get(ctx, "SIM1", "no-quantity"); err == nil || found || f != nil {
		t.Fatalf("Get(no-quantity) = %v, %v, %v; want an error", f, found, err)
	}
	if p, found, err := plans.Get(ctx, "SIM1", "day", "overflow"); !errors.Is(err, labourplan.ErrThroughputNotFinite) || found || p != nil {
		t.Fatalf("Get(overflow) = %v, %v, %v; want ErrThroughputNotFinite", p, found, err)
	}
	if listed, _, err := plans.List(ctx, "SIM1", "day", "", 10); !errors.Is(err, labourplan.ErrThroughputNotFinite) || listed != nil {
		t.Fatalf("List over a corrupt row = %v, %v; want ErrThroughputNotFinite", listed, err)
	}
}

// The tables refuse what the domain refuses (the cheap invariants), and
// migration 0014 is reversible: up, down to 0013, up again.
func TestMigration0014_ChecksAndUpDownUp(t *testing.T) {
	databaseURL := startPostgres(t)
	if err := postgres.RunMigrations(databaseURL, migrationsDir(t)); err != nil {
		t.Fatal(err)
	}
	pool, err := postgres.NewPool(context.Background(), databaseURL)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	ctx := context.Background()
	const bucket = `'[{"cpt":"2026-11-18T08:00:00Z","quantity":3}]'`
	for name, q := range map[string]string{
		"forecast version 0":     `INSERT INTO workload_forecasts VALUES ('SIM1', 'p', ` + bucket + `, now(), 0)`,
		"forecast no buckets":    `INSERT INTO workload_forecasts VALUES ('SIM1', 'p', '[]', now(), 1)`,
		"forecast object bucket": `INSERT INTO workload_forecasts VALUES ('SIM1', 'p', '{"cpt":"2026-11-18T08:00:00Z"}', now(), 1)`,
		"forecast blank site":    `INSERT INTO workload_forecasts VALUES ('', 'p', ` + bucket + `, now(), 1)`,
		"forecast no received":   `INSERT INTO workload_forecasts (site_id, path_id, buckets, version) VALUES ('SIM1', 'p', ` + bucket + `, 1)`,
		"plan version 0":         `INSERT INTO labour_plans VALUES ('SIM1', 'day', 'p', 1, 10, 8, 2, NULL, 'request', 0)`,
		"plan heads > stations":  `INSERT INTO labour_plans VALUES ('SIM1', 'day', 'p', 3, 10, 8, 2, NULL, 'request', 1)`,
		"plan negative heads":    `INSERT INTO labour_plans VALUES ('SIM1', 'day', 'p', -1, 10, 8, 2, NULL, 'request', 1)`,
		"plan zero rate":         `INSERT INTO labour_plans VALUES ('SIM1', 'day', 'p', 1, 0, 8, 2, NULL, 'request', 1)`,
		"plan NaN rate":          `INSERT INTO labour_plans VALUES ('SIM1', 'day', 'p', 1, 'NaN', 8, 2, NULL, 'request', 1)`,
		"plan infinite hours":    `INSERT INTO labour_plans VALUES ('SIM1', 'day', 'p', 1, 10, 'Infinity', 2, NULL, 'request', 1)`,
		"plan zero hours":        `INSERT INTO labour_plans VALUES ('SIM1', 'day', 'p', 1, 10, 0, 2, NULL, 'request', 1)`,
		"plan unknown source":    `INSERT INTO labour_plans VALUES ('SIM1', 'day', 'p', 1, 10, 8, 2, NULL, 'guess', 1)`,
		"plan blank shift":       `INSERT INTO labour_plans VALUES ('SIM1', '', 'p', 1, 10, 8, 2, NULL, 'request', 1)`,
		"plan heads overflow":    `INSERT INTO labour_plans VALUES ('SIM1', 'day', 'p', 2147483648, 10, 8, 2147483648, NULL, 'request', 1)`,
	} {
		if _, err := pool.Exec(ctx, q); err == nil {
			t.Errorf("%s: the table accepted a row the domain refuses", name)
		}
	}

	m, err := migrate.New("file://"+migrationsDir(t), databaseURL)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _, _ = m.Close() }()
	if err := m.Migrate(13); err != nil {
		t.Fatalf("down 0014: %v", err)
	}
	tables := func() int {
		var n int
		if err := pool.QueryRow(ctx, `SELECT count(*) FROM information_schema.tables
			WHERE table_name IN ('workload_forecasts', 'labour_plans')`).Scan(&n); err != nil {
			t.Fatal(err)
		}
		return n
	}
	if n := tables(); n != 0 {
		t.Fatalf("%d of the 0014 tables survived the down migration", n)
	}
	if err := m.Up(); err != nil {
		t.Fatalf("up again: %v", err)
	}
	if n := tables(); n != 2 {
		t.Fatalf("up again created %d of the 2 tables", n)
	}
}
