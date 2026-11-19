//go:build integration

package postgres_test

import (
	"context"
	"reflect"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/claudioed/warehouse-planning/internal/adapters/outbound/postgres"
	"github.com/claudioed/warehouse-planning/internal/domain/capacityplan"
	"github.com/claudioed/warehouse-planning/internal/domain/processcapacity"
)

// A plan's labour requirement (docs/adr/0022 decision 5) is stored with it,
// survives the publish re-save, and a plan stored before migration 0013
// reads back with none.
func TestCapacityPlanRepo_PersistsTheLabourRequirementAndLegacyRowsReadAsNone(t *testing.T) {
	pool := newMigratedPool(t)
	repo := postgres.NewCapacityPlanRepo(pool)
	ctx := context.Background()
	start := time.Date(2026, 11, 20, 8, 0, 0, 0, time.UTC)
	window, err := processcapacity.NewCapacityWindow(start, start.Add(8*time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	rate, err := processcapacity.NewCapacityRate(1000, processcapacity.UnitOrder, time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	labour := []capacityplan.LabourRequirement{
		{ProcessType: "PICK", StandardSeconds: 12, StandardSiteScoped: true, Tasks: 2400, RequiredLabourHours: 8, RequiredAverageHeads: 1, Known: true},
		{ProcessType: "REBIN", Reason: capacityplan.ReasonNoLaborStandard},
		{ProcessType: "PACK", StandardSeconds: 90.5, Reason: capacityplan.ReasonNoTaskFactor},
	}
	p, err := capacityplan.Create(capacityplan.CreateParams{
		ID: "plan-labour", WarehouseID: "WH-1", SiteID: "SIM1", Location: "SIM1", Window: window, ProcessPathID: "pick-rebin-pack",
		AssignedDemand: 10, PathRate: rate, BottleneckStep: "REBIN", LabourRequirement: labour,
	}, start.Add(-time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	if err := repo.Save(ctx, p); err != nil {
		t.Fatal(err)
	}
	got, err := repo.FindByID(ctx, "plan-labour")
	if err != nil || got == nil || !reflect.DeepEqual(got.LabourRequirement(), labour) {
		t.Fatalf("plan = %+v, %v; want labour %+v", got, err, labour)
	}
	if err := got.Publish(start.Add(-30 * time.Minute)); err != nil {
		t.Fatal(err)
	}
	if err := repo.Save(ctx, got); err != nil {
		t.Fatal(err)
	}
	if again, _ := repo.FindByID(ctx, "plan-labour"); again == nil || !reflect.DeepEqual(again.LabourRequirement(), labour) {
		t.Fatalf("publish changed the labour requirement: %+v", again)
	}

	assertNoLabourStoresAnEmptyArray(ctx, t, pool, repo, window, rate)
	assertLegacyRowReadsNoLabour(ctx, t, pool, repo, start)
}

// assertNoLabourStoresAnEmptyArray: a plan created without a requirement
// stores '[]' (the column is NOT NULL) and reads back none.
func assertNoLabourStoresAnEmptyArray(ctx context.Context, t *testing.T, pool *pgxpool.Pool, repo *postgres.CapacityPlanRepo, window processcapacity.CapacityWindow, rate processcapacity.CapacityRate) {
	t.Helper()
	empty, err := capacityplan.Create(capacityplan.CreateParams{
		ID: "plan-no-labour", WarehouseID: "WH-1", SiteID: "SIM1", Location: "SIM1", Window: window, ProcessPathID: "pick",
		AssignedDemand: 10, PathRate: rate, BottleneckStep: "PICK",
	}, window.Start().Add(-time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	if err := repo.Save(ctx, empty); err != nil {
		t.Fatal(err)
	}
	var stored string
	if err := pool.QueryRow(ctx, "SELECT labour_requirement::text FROM capacity_plans WHERE id = 'plan-no-labour'").Scan(&stored); err != nil || stored != "[]" {
		t.Fatalf("labour_requirement of a plan without one = %q, %v; want []", stored, err)
	}
	if got, _ := repo.FindByID(ctx, "plan-no-labour"); got == nil || len(got.LabourRequirement()) != 0 {
		t.Fatalf("a plan without a requirement read back as %+v", got)
	}
}

// assertLegacyRowReadsNoLabour: a row written before migration 0013 (no
// labour_requirement given) reads back with none.
func assertLegacyRowReadsNoLabour(ctx context.Context, t *testing.T, pool *pgxpool.Pool, repo *postgres.CapacityPlanRepo, start time.Time) {
	t.Helper()
	if _, err := pool.Exec(ctx, `
		INSERT INTO capacity_plans (id, warehouse_id, location, window_start, window_end, process_path_id,
			assigned_demand, path_capacity, bottleneck_step, capacity_over_window, shortage, status, created_at)
		VALUES ('legacy-labour', 'WH-1', 'SIM1', $1, $2, 'pick', 10, 1000, 'PICK', 8000, 0, 'DRAFT', $3)`,
		start, start.Add(8*time.Hour), start.Add(-time.Hour)); err != nil {
		t.Fatal(err)
	}
	legacy, err := repo.FindByID(ctx, "legacy-labour")
	if err != nil || legacy == nil || len(legacy.LabourRequirement()) != 0 {
		t.Fatalf("legacy plan = %+v, %v; want no labour requirement", legacy, err)
	}
}
