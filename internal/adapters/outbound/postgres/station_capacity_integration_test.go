//go:build integration

package postgres_test

import (
	"context"
	"reflect"
	"strconv"
	"testing"
	"time"

	"github.com/golang-migrate/migrate/v4"
	"github.com/jackc/pgx/v5/pgxpool"

	outboundkafka "github.com/claudioed/warehouse-planning/internal/adapters/outbound/kafka"
	"github.com/claudioed/warehouse-planning/internal/adapters/outbound/postgres"
	"github.com/claudioed/warehouse-planning/internal/application/tally"
	"github.com/claudioed/warehouse-planning/internal/application/usecases"
	"github.com/claudioed/warehouse-planning/internal/domain/processcapacity"
	"github.com/claudioed/warehouse-planning/internal/domain/processpath"
)

func scalar[T any](t *testing.T, pool *pgxpool.Pool, query string, args ...any) T {
	t.Helper()
	var v T
	if err := pool.QueryRow(context.Background(), query, args...).Scan(&v); err != nil {
		t.Fatalf("%s: %v", query, err)
	}
	return v
}

func mustExec(t *testing.T, pool *pgxpool.Pool, query string, args ...any) {
	t.Helper()
	if _, err := pool.Exec(context.Background(), query, args...); err != nil {
		t.Fatalf("%s: %v", query, err)
	}
}

// migrateTo returns a golang-migrate handle on databaseURL and the shared
// migrations directory; the caller drives the version.
func migrator(t *testing.T, databaseURL string) *migrate.Migrate {
	t.Helper()
	m, err := migrate.New("file://"+migrationsDir(t), databaseURL)
	if err != nil {
		t.Fatalf("migrate.New: %v", err)
	}
	t.Cleanup(func() { _, _ = m.Close() })
	return m
}

// seedLegacyRows writes what the retired facility consumer used to
// materialize (a STORAGE sentinel row and STATION rows in unit LINE on the
// 1970 standing window), next to REAL ProcessCapacity rows that must survive.
func seedLegacyRows(t *testing.T, pool *pgxpool.Pool) {
	t.Helper()
	epoch := time.Unix(0, 0).UTC()
	standingEnd := epoch.AddDate(100, 0, 0)
	w26s, w26e := time.Date(2026, 10, 5, 8, 0, 0, 0, time.UTC), time.Date(2026, 10, 5, 16, 0, 0, 0, time.UTC)

	insert := func(process, location, unit string, start, end time.Time, constraintType string, qty float64) {
		mustExec(t, pool, `INSERT INTO process_capacity (process_type, location, window_start, window_end, native_unit) VALUES ($1,$2,$3,$4,$5)`,
			process, location, start, end, unit)
		mustExec(t, pool, `INSERT INTO process_capacity_constraint (process_type, location, window_start, window_end, constraint_type, quantity, period_seconds, ordinal)
			VALUES ($1,$2,$3,$4,$5,$6,3600,0)`, process, location, start, end, constraintType, qty)
	}
	// Legacy derived rows.
	insert("STORAGE", "SIM1-STOR-AMB:SimShelf", "LINE", epoch, standingEnd, "LOCATION", 24)
	insert("PACK", "SIM1-OPS-WC", "LINE", epoch, standingEnd, "STATION", 11)
	insert("STORAGE", "SIM1-STOR-AMB:Other", "LINE", w26s, w26e, "LOCATION", 3) // sentinel type alone is enough
	// Real rows that must survive: labor for PACK at the site (same process
	// type as a legacy row, a real window) and for PICK.
	insert("PACK", "SIM1", "PACKAGE", w26s, w26e, "LABOR", 2500)
	insert("PICK", "SIM1", "UNIT", w26s, w26e, "LABOR", 8000)

	// The tally the legacy rows were derived from stays the source of truth.
	mustExec(t, pool, `INSERT INTO location_slot_tally (zone_id, tally_type, tally_key, count) VALUES ('SIM1-OPS-WC','STATION','PACK',11), ('SIM1-STOR-AMB','LOCATION','SimShelf',24)`)
	mustExec(t, pool, `INSERT INTO location_slot_registration (location_code, zone_id, tally_type, tally_keys) VALUES ('SIM1-OPS-WC-1','SIM1-OPS-WC','STATION','{PACK}')`)
	// A plan created before 0005.
	mustExec(t, pool, `INSERT INTO capacity_plans (id, warehouse_id, location, window_start, window_end, process_path_id, assigned_demand, path_capacity,
		bottleneck_step, capacity_over_window, shortage, status, created_at) VALUES ('legacy-plan','WH-1','SIM1',$1,$2,'pick-rebin-pack',12000,1000,'REBIN',8000,4000,'DRAFT',now())`, w26s, w26e)
}

// Migration 0005 applies on top of 0004, deletes the legacy derived rows (the
// constraints cascade) and ONLY those, leaves the tally and real rows, gives
// existing plans empty composition fields, and its down migration drops what
// it added and nothing else. It is idempotent to re-apply.
func TestMigration0005_AppliesOn0004_DeletesLegacyDerivedRows_AndRollsBack(t *testing.T) {
	databaseURL := startPostgres(t)
	m := migrator(t, databaseURL)
	if err := m.Migrate(4); err != nil {
		t.Fatalf("migrate to 0004: %v", err)
	}
	ctx := context.Background()
	pool, err := postgres.NewPool(ctx, databaseURL)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	if scalar[bool](t, pool, `SELECT to_regclass('station_standards') IS NOT NULL`) {
		t.Fatal("station_standards exists before 0005")
	}
	seedLegacyRows(t, pool)
	if n := scalar[int](t, pool, `SELECT count(*) FROM process_capacity`); n != 5 {
		t.Fatalf("seeded %d process_capacity rows, want 5", n)
	}

	if err := postgres.RunMigrations(databaseURL, migrationsDir(t)); err != nil {
		t.Fatalf("apply 0005 on top of 0004: %v", err)
	}

	// Legacy derived rows are gone, with their constraints.
	if n := scalar[int](t, pool, `SELECT count(*) FROM process_capacity WHERE process_type='STORAGE' OR window_start = '1970-01-01T00:00:00Z'`); n != 0 {
		t.Errorf("%d legacy process_capacity rows survived", n)
	}
	if n := scalar[int](t, pool, `SELECT count(*) FROM process_capacity_constraint WHERE process_type='STORAGE' OR window_start = '1970-01-01T00:00:00Z'`); n != 0 {
		t.Errorf("%d legacy constraint rows survived (the cascade must remove them)", n)
	}
	// Real rows and the tally survive untouched.
	if n := scalar[int](t, pool, `SELECT count(*) FROM process_capacity WHERE location='SIM1' AND process_type IN ('PACK','PICK')`); n != 2 {
		t.Errorf("real labor rows = %d, want both kept", n)
	}
	if n := scalar[int](t, pool, `SELECT count(*) FROM process_capacity_constraint WHERE constraint_type='LABOR'`); n != 2 {
		t.Errorf("real LABOR constraints = %d, want 2", n)
	}
	if n := scalar[int](t, pool, `SELECT count(*) FROM location_slot_tally`); n != 2 {
		t.Errorf("tally rows = %d, want 2 (the tally is the source of truth and must be kept)", n)
	}
	if n := scalar[int](t, pool, `SELECT count(*) FROM location_slot_registration`); n != 1 {
		t.Errorf("slot registrations = %d, want 1", n)
	}
	// New table + plan columns.
	if n := scalar[int](t, pool, `SELECT count(*) FROM station_standards`); n != 0 {
		t.Errorf("station_standards has %d rows, want an empty new table", n)
	}
	if bc, w := scalar[string](t, pool, `SELECT bottleneck_constraint FROM capacity_plans WHERE id='legacy-plan'`),
		scalar[int](t, pool, `SELECT cardinality(warnings) FROM capacity_plans WHERE id='legacy-plan'`); bc != "" || w != 0 {
		t.Errorf("legacy plan composition = %q/%d, want empty defaults", bc, w)
	}

	// Re-applying is a no-op.
	if err := postgres.RunMigrations(databaseURL, migrationsDir(t)); err != nil {
		t.Fatalf("re-applying migrations: %v", err)
	}

	// Down drops only what 0005 added. Migrate(4) rather than Steps(-1): later
	// migrations (0006 and on) sit above 0005, and they are rolled back first.
	if err := migrator(t, databaseURL).Migrate(4); err != nil {
		t.Fatalf("down to 0004: %v", err)
	}
	if scalar[bool](t, pool, `SELECT to_regclass('station_standards') IS NOT NULL`) {
		t.Error("station_standards still exists after the down migration")
	}
	if n := scalar[int](t, pool, `SELECT count(*) FROM information_schema.columns WHERE table_name='capacity_plans' AND column_name IN ('bottleneck_constraint','warnings')`); n != 0 {
		t.Errorf("%d composition columns remain after down", n)
	}
	if n := scalar[int](t, pool, `SELECT count(*) FROM capacity_plans`) + scalar[int](t, pool, `SELECT count(*) FROM location_slot_tally`) + scalar[int](t, pool, `SELECT count(*) FROM process_capacity`); n != 1+2+2 {
		t.Errorf("down touched data: %d rows, want 5", n)
	}
	if err := postgres.RunMigrations(databaseURL, migrationsDir(t)); err != nil {
		t.Fatalf("up again after down: %v", err)
	}
}

func standardOf(t *testing.T, location string, process processcapacity.ProcessType, qty float64, unit processcapacity.CapacityUnit, period time.Duration) processcapacity.StationStandard {
	t.Helper()
	rate, err := processcapacity.NewCapacityRate(qty, unit, period)
	if err != nil {
		t.Fatal(err)
	}
	s, err := processcapacity.NewStationStandard(location, process, rate)
	if err != nil {
		t.Fatal(err)
	}
	return s
}

func TestStationStandardRepo_Postgres_RoundTrip(t *testing.T) {
	pool := newMigratedPool(t)
	repo := postgres.NewStationStandardRepo(pool)
	ctx := context.Background()

	if got, err := repo.Find(ctx, "SIM1", "PACK"); err != nil || got != nil {
		t.Fatalf("Find on empty = %v, %v, want nil, nil", got, err)
	}
	for _, s := range []processcapacity.StationStandard{
		standardOf(t, "SIM2", "PACK", 75, processcapacity.UnitOrder, time.Hour),
		standardOf(t, "SIM1", "SORT", 90.5, processcapacity.UnitUnit, 30*time.Minute),
		standardOf(t, "SIM1", "PACK", 180, processcapacity.UnitPackage, time.Hour),
	} {
		if err := repo.Save(ctx, s); err != nil {
			t.Fatal(err)
		}
	}
	got, err := repo.Find(ctx, "SIM1", "SORT")
	if err != nil || got == nil {
		t.Fatalf("Find = %v, %v", got, err)
	}
	if r := got.PerStation(); r.Quantity() != 90.5 || r.Unit() != processcapacity.UnitUnit || r.Period() != 30*time.Minute {
		t.Fatalf("round trip = %v %s per %v, want 90.5 UNIT per 30m", r.Quantity(), r.Unit(), r.Period())
	}

	// Upsert replaces the throughput and refreshes updated_at.
	before := scalar[time.Time](t, pool, `SELECT updated_at FROM station_standards WHERE location='SIM1' AND process_type='PACK'`)
	if err := repo.Save(ctx, standardOf(t, "SIM1", "PACK", 200, processcapacity.UnitPackage, time.Hour)); err != nil {
		t.Fatal(err)
	}
	if after := scalar[time.Time](t, pool, `SELECT updated_at FROM station_standards WHERE location='SIM1' AND process_type='PACK'`); !after.After(before) {
		t.Errorf("updated_at not refreshed on replace: %v -> %v", before, after)
	}
	if n := scalar[int](t, pool, `SELECT count(*) FROM station_standards`); n != 3 {
		t.Fatalf("%d rows, want 3 (upsert must not duplicate)", n)
	}

	keys := func(l []processcapacity.StationStandard) []string {
		var out []string
		for _, s := range l {
			out = append(out, s.Location()+"/"+string(s.ProcessType())+"="+strconv.FormatFloat(s.PerStation().Quantity(), 'f', -1, 64))
		}
		return out
	}
	site, _ := repo.List(ctx, "SIM1")
	if want := []string{"SIM1/PACK=200", "SIM1/SORT=90.5"}; !reflect.DeepEqual(keys(site), want) {
		t.Errorf("List(SIM1) = %v, want %v", keys(site), want)
	}
	all, _ := repo.List(ctx, "")
	if want := []string{"SIM1/PACK=200", "SIM1/SORT=90.5", "SIM2/PACK=75"}; !reflect.DeepEqual(keys(all), want) {
		t.Errorf("List(all) = %v, want %v", keys(all), want)
	}
	if none, err := repo.List(ctx, "NOWHERE"); err != nil || none == nil || len(none) != 0 {
		t.Errorf("List(NOWHERE) = %v, %v, want an empty non-nil slice", none, err)
	}

	// The table itself refuses a non-positive throughput.
	if _, err := pool.Exec(ctx, `INSERT INTO station_standards (location, process_type, quantity, unit, period_seconds) VALUES ('X','Y',0,'UNIT',3600)`); err == nil {
		t.Error("a zero quantity was accepted by the table; the CHECK must refuse it")
	}
}

func TestStationStandardRepo_Postgres_JoinsTheUnitOfWork(t *testing.T) {
	pool := newMigratedPool(t)
	repo, uow := postgres.NewStationStandardRepo(pool), postgres.NewUnitOfWork(pool)
	ctx := context.Background()
	errRollback := context.Canceled
	err := uow.Do(ctx, func(ctx context.Context) error {
		if err := repo.Save(ctx, standardOf(t, "SIM1", "PACK", 180, processcapacity.UnitPackage, time.Hour)); err != nil {
			return err
		}
		return errRollback
	})
	if err != errRollback {
		t.Fatalf("Do = %v, want the rollback error", err)
	}
	if got, _ := repo.Find(ctx, "SIM1", "PACK"); got != nil {
		t.Fatal("a standard saved inside a rolled-back unit of work was kept")
	}
}

func seedTallySlots(t *testing.T, repo *postgres.StorageTallyRepo) {
	t.Helper()
	ctx := context.Background()
	regs := []struct{ code, zone, typ, key string }{
		{"a1", "SIM1-OPS-WC", tally.TypeStation, "PACK"}, {"a2", "SIM1-OPS-WC", tally.TypeStation, "PACK"},
		{"a3", "SIM1-OPS-WC2", tally.TypeStation, "PACK"}, {"a4", "SIM1-OPS-WC", tally.TypeStation, "SORT"},
		{"b1", "SIM1-STOR-AMB", tally.TypeLocation, "SimShelf"}, {"b2", "SIM1-STOR-AMB", tally.TypeLocation, "SimShelf"},
		{"c1", "SIM10-OPS-WC", tally.TypeStation, "PACK"}, {"d1", "SIM2-OPS-WC", tally.TypeStation, "PACK"},
		{"e1", "SIM1-GONE", tally.TypeStation, "PACK"},
		{"f1", "SIMX-OPS-WC", tally.TypeStation, "PACK"}, // must not match a location written as a LIKE pattern
	}
	for _, r := range regs {
		if _, err := repo.RegisterSlot(ctx, r.code, r.zone, r.typ, []string{r.key}); err != nil {
			t.Fatal(err)
		}
	}
	if _, _, err := repo.DecommissionSlot(ctx, "e1"); err != nil {
		t.Fatal(err)
	}
}

func TestStorageTallyReader_Postgres_SiteScopedReads(t *testing.T) {
	pool := newMigratedPool(t)
	repo := postgres.NewStorageTallyRepo(pool)
	seedTallySlots(t, repo)
	ctx := context.Background()

	for _, tc := range []struct {
		location, activity string
		want               int
	}{
		{"SIM1", "PACK", 3}, {"SIM1", "SORT", 1}, {"SIM1", "QC", 0}, {"SIM10", "PACK", 1},
		{"SIM", "PACK", 0}, {"SIM1", "SimShelf", 0},
		{"SIM_", "PACK", 0}, {"SIM%", "PACK", 0}, // a location is never a LIKE pattern
	} {
		got, err := repo.StationCount(ctx, tc.location, tc.activity)
		if err != nil || got != tc.want {
			t.Errorf("StationCount(%s, %s) = %d, %v, want %d", tc.location, tc.activity, got, err, tc.want)
		}
	}

	got, err := repo.SiteBuckets(ctx, "SIM1")
	if err != nil {
		t.Fatal(err)
	}
	want := []tally.Bucket{
		{ZoneID: "SIM1-OPS-WC", TallyType: tally.TypeStation, TallyKey: "PACK", Count: 2},
		{ZoneID: "SIM1-OPS-WC", TallyType: tally.TypeStation, TallyKey: "SORT", Count: 1},
		{ZoneID: "SIM1-OPS-WC2", TallyType: tally.TypeStation, TallyKey: "PACK", Count: 1},
		{ZoneID: "SIM1-STOR-AMB", TallyType: tally.TypeLocation, TallyKey: "SimShelf", Count: 2},
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("SiteBuckets = %v, want %v", got, want)
	}
	if none, err := repo.SiteBuckets(ctx, "NOWHERE"); err != nil || none == nil || len(none) != 0 {
		t.Errorf("SiteBuckets(NOWHERE) = %v, %v, want an empty non-nil slice", none, err)
	}
}

// The composed path capacity and the capacity plans against REAL Postgres,
// through the same use cases the services wire: fixtures A+B (and C's warning,
// persisted with the plan and read back).
func TestComposedPathCapacity_RealPostgres_FixturesBandC(t *testing.T) {
	pool := newMigratedPool(t)
	ctx := context.Background()
	pcs, paths := postgres.NewProcessCapacityRepo(pool), postgres.NewProcessPathRepo(pool)
	standards, tallyRepo := postgres.NewStationStandardRepo(pool), postgres.NewStorageTallyRepo(pool)
	plans, ob, uow := postgres.NewCapacityPlanRepo(pool), postgres.NewOutboxRepo(pool), postgres.NewUnitOfWork(pool)

	register := &usecases.RegisterProcessCapacityConstraint{Repo: pcs}
	labor := func(process processcapacity.ProcessType, qty float64, unit processcapacity.CapacityUnit) {
		t.Helper()
		if _, err := register.Handle(ctx, usecases.RegisterProcessCapacityConstraintCommand{
			ProcessType: process, Location: "SIM1", WindowStart: planStart, WindowEnd: planEnd,
			ConstraintType: processcapacity.ConstraintLabor, Quantity: qty, Unit: unit, Period: time.Hour,
		}); err != nil {
			t.Fatal(err)
		}
	}
	labor("PICK", 8000, processcapacity.UnitUnit)
	labor("REBIN", 2500, processcapacity.UnitUnit)
	labor("PACK", 2500, processcapacity.UnitPackage)
	if _, err := (&usecases.RegisterProcessPath{Repo: paths}).Handle(ctx, usecases.RegisterProcessPathCommand{
		ID: "pick-rebin-pack", Name: "Pick-Rebin-Pack", Steps: []processpath.ProcessType{"PICK", "REBIN", "PACK"},
	}); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 10; i++ {
		if _, err := tallyRepo.RegisterSlot(ctx, "SIM1-OPS-WC-"+strconv.Itoa(i), "SIM1-OPS-WC", tally.TypeStation, []string{"PACK"}); err != nil {
			t.Fatal(err)
		}
	}
	// Stations of other sites never leak in.
	if _, err := tallyRepo.RegisterSlot(ctx, "SIM10-OPS-WC-0", "SIM10-OPS-WC", tally.TypeStation, []string{"PACK"}); err != nil {
		t.Fatal(err)
	}

	pathCapacity := &usecases.GetProcessPathCapacity{ProcessPaths: paths, ProcessCapacities: pcs, StationStandards: standards, Tally: tallyRepo}
	upo, ppo := 2.5, 1.0
	get := func() usecases.GetProcessPathCapacityResult {
		t.Helper()
		res, err := pathCapacity.Handle(ctx, usecases.GetProcessPathCapacityCommand{
			ProcessPathID: "pick-rebin-pack", Location: "SIM1", WindowStart: planStart, WindowEnd: planEnd, UnitsPerOrder: &upo, PackagesPerOrder: &ppo,
		})
		if err != nil {
			t.Fatal(err)
		}
		return res
	}
	rates := func(r usecases.GetProcessPathCapacityResult) []float64 {
		var out []float64
		for _, s := range r.Steps {
			out = append(out, s.Rate.Quantity())
		}
		return out
	}
	create := &usecases.CreateCapacityPlan{
		PathCapacity: pathCapacity, Plans: plans, Outbox: ob, Encoder: outboundkafka.NewEncoder(), UnitOfWork: uow,
	}
	planCmd := usecases.CreateCapacityPlanCommand{
		WarehouseID: "WH-1", Location: "SIM1", WindowStart: planStart, WindowEnd: planEnd, ProcessPathID: "pick-rebin-pack",
		AssignedDemand: 20000, UnitsPerOrder: &upo, PackagesPerOrder: &ppo,
	}

	// FIXTURE C: stations tallied, no standard: labor only + a warning, stored with the plan.
	c := get()
	if c.NormalizedRate.Quantity() != 1000 || len(c.Warnings) != 1 || c.Steps[2].Binding != processcapacity.ConstraintLabor {
		t.Fatalf("no standard: %v warnings %v, want 1000 with one warning and PACK bound by LABOR", c.NormalizedRate.Quantity(), c.Warnings)
	}
	planC, err := create.Handle(ctx, planCmd)
	if err != nil {
		t.Fatal(err)
	}
	storedC, err := plans.FindByID(ctx, planC.ID())
	if err != nil || storedC == nil || !reflect.DeepEqual(storedC.Warnings(), planC.Warnings()) || len(storedC.Warnings()) != 1 {
		t.Fatalf("stored fixture C plan = %v (err %v), want its warning round-tripped", storedC, err)
	}

	// FIXTURE B step 1: Rebin bottlenecks at 1000 (3200/1000/1800).
	if _, _, err := (&usecases.DeclareStationStandard{Repo: standards}).Handle(ctx, usecases.DeclareStationStandardCommand{
		Location: "SIM1", ProcessType: "PACK", Quantity: 180, Unit: processcapacity.UnitPackage, Period: time.Hour,
	}); err != nil {
		t.Fatal(err)
	}
	b := get()
	if !reflect.DeepEqual(rates(b), []float64{3200, 1000, 1800}) || b.NormalizedRate.Quantity() != 1000 || b.BottleneckStep != "REBIN" || len(b.Warnings) != 0 {
		t.Fatalf("fixture B step 1: %v %v %s warnings %v", rates(b), b.NormalizedRate.Quantity(), b.BottleneckStep, b.Warnings)
	}

	// FIXTURE B step 2: Rebin raised to 6000 UNIT/h -> 3200/2400/1800, PACK/STATION binds at 1800.
	labor("REBIN", 6000, processcapacity.UnitUnit)
	b = get()
	if !reflect.DeepEqual(rates(b), []float64{3200, 2400, 1800}) || b.NormalizedRate.Quantity() != 1800 ||
		b.BottleneckStep != "PACK" || b.BottleneckConstraint != processcapacity.ConstraintStation {
		t.Fatalf("fixture B step 2: %v %v %s/%s", rates(b), b.NormalizedRate.Quantity(), b.BottleneckStep, b.BottleneckConstraint)
	}

	plan, err := create.Handle(ctx, planCmd)
	if err != nil {
		t.Fatal(err)
	}
	stored, err := plans.FindByID(ctx, plan.ID())
	if err != nil || stored == nil {
		t.Fatalf("FindByID = %v, %v", stored, err)
	}
	if stored.PathCapacity() != 1800 || stored.Shortage() != 5600 || stored.BottleneckStep() != "PACK" ||
		stored.BottleneckConstraint() != processcapacity.ConstraintStation || len(stored.Warnings()) != 0 {
		t.Fatalf("stored plan = %v short %v %s/%s warnings %v", stored.PathCapacity(), stored.Shortage(), stored.BottleneckStep(), stored.BottleneckConstraint(), stored.Warnings())
	}
	// The published event payloads did not grow: still exactly the Created event for each plan.
	if n := scalar[int](t, pool, `SELECT count(*) FROM outbox_events`); n != 2 {
		t.Errorf("outbox rows = %d, want 2 (one CapacityPlanCreated per plan, no new events)", n)
	}
}
