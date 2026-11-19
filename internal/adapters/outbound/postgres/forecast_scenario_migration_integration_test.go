//go:build integration

package postgres_test

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/golang-migrate/migrate/v4"

	"github.com/claudioed/warehouse-planning/internal/adapters/outbound/postgres"
	"github.com/claudioed/warehouse-planning/internal/domain/capacityplan"
	"github.com/claudioed/warehouse-planning/internal/domain/processcapacity"
)

// Migration 0016 (docs/adr/0024): demand_source accepts forecast,
// forecast_path_id is never blank, a scenario is 1 to 64 characters, and the
// migration is reversible -- down to 0015 a forecast plan reads back as a
// request plan with its assigned_demand kept, up again restores the columns.
func TestMigration0016_ChecksAndUpDownUp(t *testing.T) {
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
	start := time.Date(2026, 10, 5, 8, 0, 0, 0, time.UTC)
	window, err := processcapacity.NewCapacityWindow(start, start.Add(8*time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	repo := postgres.NewCapacityPlanRepo(pool)
	plan := capacityplan.Rehydrate(capacityplan.RehydrateParams{
		ID: "plan-forecast", WarehouseID: "WH-1", SiteID: "SIM1", Location: "SIM1", Window: window, ProcessPathID: "pick-only",
		AssignedDemand: 41, PathCapacity: 10, BottleneckStep: "PICK", CapacityOverWindow: 80,
		Status: capacityplan.StatusDraft, CreatedAt: start.Add(-time.Hour),
		DemandSource: capacityplan.DemandSourceForecast, ForecastPathID: "PICK", Scenario: strings.Repeat("x", 64),
		BottleneckConstraint: processcapacity.ConstraintLabor, Warnings: []string{},
	})
	if err := repo.Save(ctx, plan); err != nil {
		t.Fatalf("a forecast plan with a 64-character scenario: %v", err)
	}
	stored, err := repo.FindByID(ctx, "plan-forecast")
	if err != nil || stored.DemandSource() != capacityplan.DemandSourceForecast || stored.ForecastPathID() != "PICK" || stored.Scenario() != plan.Scenario() {
		t.Fatalf("round trip = %+v, %v", stored, err)
	}
	for name, q := range map[string]string{
		"an unknown demand source": `UPDATE capacity_plans SET demand_source = 'inbound' WHERE id = 'plan-forecast'`,
		"a blank forecast path":    `UPDATE capacity_plans SET forecast_path_id = '' WHERE id = 'plan-forecast'`,
		"an empty scenario":        `UPDATE capacity_plans SET scenario = '' WHERE id = 'plan-forecast'`,
		"a 65-character scenario":  `UPDATE capacity_plans SET scenario = repeat('x', 65) WHERE id = 'plan-forecast'`,
	} {
		if _, err := pool.Exec(ctx, q); err == nil {
			t.Errorf("%s: the table accepted a value the domain refuses", name)
		}
	}

	m, err := migrate.New("file://"+migrationsDir(t), databaseURL)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _, _ = m.Close() }()
	if err := m.Migrate(15); err != nil {
		t.Fatalf("down 0016: %v", err)
	}
	var source string
	var demand float64
	if err := pool.QueryRow(ctx, `SELECT demand_source, assigned_demand FROM capacity_plans WHERE id = 'plan-forecast'`).Scan(&source, &demand); err != nil {
		t.Fatal(err)
	}
	if source != "request" || demand != 41 {
		t.Fatalf("down 0016: source %q demand %v, want request 41", source, demand)
	}
	if _, err := pool.Exec(ctx, `UPDATE capacity_plans SET demand_source = 'forecast'`); err == nil {
		t.Fatal("down 0016 kept forecast in the demand_source CHECK")
	}
	if err := m.Up(); err != nil {
		t.Fatalf("up again: %v", err)
	}
	if _, err := pool.Exec(ctx, `UPDATE capacity_plans SET demand_source = 'forecast', forecast_path_id = 'PICK', scenario = 'baseline'`); err != nil {
		t.Fatalf("up again did not restore the 0016 columns: %v", err)
	}
}
