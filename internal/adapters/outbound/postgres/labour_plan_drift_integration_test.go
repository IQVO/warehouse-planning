//go:build integration

package postgres_test

import (
	"context"
	"testing"

	"github.com/golang-migrate/migrate/v4"

	"github.com/claudioed/warehouse-planning/internal/adapters/outbound/postgres"
)

// The commitments table refuses what the drift refuses (a blank key, negative
// heads, no observation time), and migration 0015 is reversible: down to
// 0014 drops only labour_plan_commitments, up again recreates it.
func TestMigration0015_ChecksAndUpDownUp(t *testing.T) {
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
	for name, q := range map[string]string{
		"blank site":      `INSERT INTO labour_plan_commitments VALUES ('', 'day', 'p', 1, now(), NULL)`,
		"blank shift":     `INSERT INTO labour_plan_commitments VALUES ('SIM1', '', 'p', 1, now(), NULL)`,
		"blank path":      `INSERT INTO labour_plan_commitments VALUES ('SIM1', 'day', '', 1, now(), NULL)`,
		"negative heads":  `INSERT INTO labour_plan_commitments VALUES ('SIM1', 'day', 'p', -1, now(), NULL)`,
		"heads overflow":  `INSERT INTO labour_plan_commitments VALUES ('SIM1', 'day', 'p', 2147483648, now(), NULL)`,
		"no observed_at":  `INSERT INTO labour_plan_commitments (site_id, shift_id, path_id, committed_heads) VALUES ('SIM1', 'day', 'p', 1)`,
		"no heads at all": `INSERT INTO labour_plan_commitments (site_id, shift_id, path_id, observed_at) VALUES ('SIM1', 'day', 'p', now())`,
	} {
		if _, err := pool.Exec(ctx, q); err == nil {
			t.Errorf("%s: the table accepted a row the drift refuses", name)
		}
	}
	if _, err := pool.Exec(ctx, `INSERT INTO labour_plan_commitments VALUES ('SIM1', 'day', 'p', 0, now(), -4)`); err != nil {
		t.Fatalf("a zero commitment with a negative recorded drift is valid: %v", err)
	}

	m, err := migrate.New("file://"+migrationsDir(t), databaseURL)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _, _ = m.Close() }()
	if err := m.Migrate(14); err != nil {
		t.Fatalf("down 0015: %v", err)
	}
	count := func(table string) int {
		var n int
		if err := pool.QueryRow(ctx, `SELECT count(*) FROM information_schema.tables WHERE table_name = $1`, table).Scan(&n); err != nil {
			t.Fatal(err)
		}
		return n
	}
	if count("labour_plan_commitments") != 0 {
		t.Fatal("labour_plan_commitments survived the down migration")
	}
	if count("labour_plans") != 1 {
		t.Fatal("down 0015 dropped labour_plans: the migration is not additive")
	}
	if err := m.Up(); err != nil {
		t.Fatalf("up again: %v", err)
	}
	if count("labour_plan_commitments") != 1 {
		t.Fatal("up again did not recreate labour_plan_commitments")
	}
}
