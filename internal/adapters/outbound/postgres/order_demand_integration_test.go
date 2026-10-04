//go:build integration

package postgres_test

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/golang-migrate/migrate/v4"

	"github.com/claudioed/warehouse-planning/internal/adapters/outbound/memory"
	"github.com/claudioed/warehouse-planning/internal/adapters/outbound/postgres"
	"github.com/claudioed/warehouse-planning/internal/domain/capacityplan"
	"github.com/claudioed/warehouse-planning/internal/domain/demand"
	"github.com/claudioed/warehouse-planning/internal/domain/processcapacity"
)

var (
	odStart = time.Date(2026, 10, 5, 8, 0, 0, 0, time.UTC)
	odEnd   = time.Date(2026, 10, 5, 16, 0, 0, 0, time.UTC)
	odAsOf  = time.Date(2026, 10, 4, 9, 15, 30, 0, time.UTC)
)

func odOrder(t *testing.T, id, location string, promise time.Time, lines int, asOf time.Time) demand.Order {
	t.Helper()
	o, err := demand.NewOrder(demand.OrderParams{OrderID: id, Location: location, PromiseAt: promise, ReleasedLines: lines, AsOf: asOf})
	if err != nil {
		t.Fatal(err)
	}
	return o
}

// The SQL window predicate must agree with demand.Order.CountsIn at every
// boundary: a cutoff at start counts, a cutoff at end does not.
func TestOrderDemandRepo_ExpectedWindowBoundariesMatchTheDomainRule(t *testing.T) {
	pool := newMigratedPool(t)
	repo := postgres.NewOrderDemandRepo(pool)
	ctx := context.Background()
	orders := []demand.Order{
		odOrder(t, "before", "SIM1", odStart.Add(-time.Second), 1, odAsOf),
		odOrder(t, "at-start", "SIM1", odStart, 2, odAsOf.Add(time.Minute)),
		odOrder(t, "last-second", "SIM1", odEnd.Add(-time.Second), 3, odAsOf.Add(2*time.Minute)),
		odOrder(t, "at-end", "SIM1", odEnd, 5, odAsOf.Add(time.Hour)),
		odOrder(t, "after", "SIM1", odEnd.Add(time.Second), 11, odAsOf.Add(3*time.Minute)),
		odOrder(t, "other-site", "SIM2", odStart.Add(time.Hour), 7, odAsOf.Add(2*time.Hour)),
	}
	mem := memory.NewOrderDemandRepo()
	for _, o := range orders {
		if applied, err := repo.Upsert(ctx, o); err != nil || !applied {
			t.Fatalf("Upsert(%s) = %v, %v", o.ID(), applied, err)
		}
		if _, err := mem.Upsert(ctx, o); err != nil {
			t.Fatal(err)
		}
	}

	got, err := repo.Expected(ctx, "SIM1", odStart, odEnd)
	if err != nil {
		t.Fatal(err)
	}
	if got.Orders != 2 || got.ReleasedLines != 2+3 {
		t.Fatalf("summary = %+v, want the orders at start and one second before end", got)
	}
	if !got.AsOf.Equal(odAsOf.Add(time.Hour)) {
		t.Fatalf("AsOf = %v, want the site's newest event (the one at the window end), not the other site's", got.AsOf)
	}
	want, _ := mem.Expected(ctx, "SIM1", odStart, odEnd)
	if got != want {
		t.Fatalf("postgres %+v != in-memory %+v: the two adapters disagree", got, want)
	}

	empty, err := repo.Expected(ctx, "NOWHERE", odStart, odEnd)
	if err != nil || empty.Orders != 0 || !empty.AsOf.IsZero() {
		t.Fatalf("unknown site = %+v, %v; want the zero Summary", empty, err)
	}
}

// Last-writer-wins must be the same function in both adapters.
func TestOrderDemandRepo_LastWriterWinsLikeTheInMemoryAdapter(t *testing.T) {
	pool := newMigratedPool(t)
	pg := postgres.NewOrderDemandRepo(pool)
	mem := memory.NewOrderDemandRepo()
	ctx := context.Background()

	steps := []struct {
		name        string
		promise     time.Time
		lines       int
		asOf        time.Time
		wantApplied bool
	}{
		{"first write", odStart.Add(time.Hour), 2, odAsOf, true},
		{"older event does not replace", odEnd.Add(time.Hour), 9, odAsOf.Add(-time.Nanosecond * 1000), false},
		{"equal time replaces", odStart.Add(2 * time.Hour), 3, odAsOf, true},
		{"newer event replaces and moves the window", odEnd.Add(2 * time.Hour), 4, odAsOf.Add(time.Second), true},
		{"older again", odStart, 8, odAsOf, false},
	}
	for _, s := range steps {
		o := odOrder(t, "ord-1", "SIM1", s.promise, s.lines, s.asOf)
		gotPg, err := pg.Upsert(ctx, o)
		if err != nil {
			t.Fatalf("%s: %v", s.name, err)
		}
		gotMem, _ := mem.Upsert(ctx, o)
		if gotPg != s.wantApplied || gotMem != s.wantApplied {
			t.Fatalf("%s: applied pg=%v mem=%v, want %v", s.name, gotPg, gotMem, s.wantApplied)
		}
	}
	var rows int
	if err := pool.QueryRow(ctx, "SELECT count(*) FROM order_demand").Scan(&rows); err != nil || rows != 1 {
		t.Fatalf("rows = %d, %v; one order id must stay one row", rows, err)
	}
	// The final state is the last NEWER write: it moved out of the first window.
	for _, w := range [][2]time.Time{{odStart, odEnd}, {odEnd, odEnd.Add(8 * time.Hour)}} {
		a, _ := pg.Expected(ctx, "SIM1", w[0], w[1])
		b, _ := mem.Expected(ctx, "SIM1", w[0], w[1])
		if a != b {
			t.Fatalf("window %v: postgres %+v != in-memory %+v", w, a, b)
		}
	}
}

// The consumer's claim and the upsert are ONE transaction: a failure after
// both rolls both back; a success commits both.
func TestOrderDemandRepo_ClaimAndUpsertCommitOrRollBackTogether(t *testing.T) {
	pool := newMigratedPool(t)
	uow := postgres.NewUnitOfWork(pool)
	repo := postgres.NewOrderDemandRepo(pool)
	processed := postgres.NewProcessedEventRepo(pool)
	ctx := context.Background()
	o := odOrder(t, "ord-1", "SIM1", odStart.Add(time.Hour), 2, odAsOf)
	boom := errors.New("injected failure after the write")

	err := uow.Do(ctx, func(ctx context.Context) error {
		if claimed, err := processed.Claim(ctx, "order-demand-consumer", "evt-1"); err != nil || !claimed {
			return fmt.Errorf("claim = %v, %v", claimed, err)
		}
		if _, err := repo.Upsert(ctx, o); err != nil {
			return err
		}
		return boom
	})
	if !errors.Is(err, boom) {
		t.Fatalf("err = %v", err)
	}
	count := func(q string) int {
		var n int
		if err := pool.QueryRow(ctx, q).Scan(&n); err != nil {
			t.Fatal(err)
		}
		return n
	}
	if count("SELECT count(*) FROM order_demand") != 0 || count("SELECT count(*) FROM processed_events WHERE consumer='order-demand-consumer'") != 0 {
		t.Fatal("a failed unit of work left an order or a claim behind")
	}

	if err := uow.Do(ctx, func(ctx context.Context) error {
		if _, err := processed.Claim(ctx, "order-demand-consumer", "evt-1"); err != nil {
			return err
		}
		_, err := repo.Upsert(ctx, o)
		return err
	}); err != nil {
		t.Fatal(err)
	}
	if count("SELECT count(*) FROM order_demand") != 1 || count("SELECT count(*) FROM processed_events WHERE consumer='order-demand-consumer'") != 1 {
		t.Fatal("a successful unit of work did not commit both rows")
	}
}

func TestMigration0006_DownIsCleanAndUpIsRepeatable(t *testing.T) {
	databaseURL := startPostgres(t)
	if err := postgres.RunMigrations(databaseURL, migrationsDir(t)); err != nil {
		t.Fatal(err)
	}
	m, err := migrate.New("file://"+migrationsDir(t), databaseURL)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _, _ = m.Close() }()

	version, dirty, err := m.Version()
	if err != nil || dirty || version != 7 {
		t.Fatalf("version = %d dirty=%v err=%v, want 7 (the latest migration is 0007_outbox_event_id_per_topic)", version, dirty, err)
	}
	// 0007 (outbox identity per topic) sits on top of 0006: undo it first.
	if err := m.Steps(-1); err != nil {
		t.Fatalf("down 0007: %v", err)
	}
	if version, dirty, err = m.Version(); err != nil || dirty || version != 6 {
		t.Fatalf("version after down 0007 = %d dirty=%v err=%v, want 6", version, dirty, err)
	}
	if err := m.Steps(-1); err != nil {
		t.Fatalf("down 0006: %v", err)
	}
	pool, err := postgres.NewPool(context.Background(), databaseURL)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	var table, column int
	if err := pool.QueryRow(context.Background(), "SELECT count(*) FROM information_schema.tables WHERE table_name = 'order_demand'").Scan(&table); err != nil || table != 0 {
		t.Fatalf("order_demand survived the down migration: %d, %v", table, err)
	}
	if err := pool.QueryRow(context.Background(), "SELECT count(*) FROM information_schema.columns WHERE table_name = 'capacity_plans' AND column_name = 'demand_source'").Scan(&column); err != nil || column != 0 {
		t.Fatalf("demand_source survived the down migration: %d, %v", column, err)
	}
	if err := m.Up(); err != nil {
		t.Fatalf("up again: %v", err)
	}
}

func TestCapacityPlanRepo_PersistsDemandSourceAndLegacyRowsReadAsRequest(t *testing.T) {
	pool := newMigratedPool(t)
	repo := postgres.NewCapacityPlanRepo(pool)
	ctx := context.Background()
	window, err := processcapacity.NewCapacityWindow(odStart, odEnd)
	if err != nil {
		t.Fatal(err)
	}
	rate, err := processcapacity.NewCapacityRate(1000, processcapacity.UnitOrder, time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	for id, source := range map[string]capacityplan.DemandSource{
		"plan-orders": capacityplan.DemandSourceOrders, "plan-request": capacityplan.DemandSourceRequest, "plan-default": "",
	} {
		p, err := capacityplan.Create(capacityplan.CreateParams{
			ID: id, WarehouseID: "WH-1", Location: "SIM1", Window: window, ProcessPathID: "pick",
			AssignedDemand: 8500, PathRate: rate, BottleneckStep: "PICK", DemandSource: source,
		}, odAsOf)
		if err != nil {
			t.Fatal(err)
		}
		if err := repo.Save(ctx, p); err != nil {
			t.Fatalf("Save %s: %v", id, err)
		}
	}
	want := map[string]capacityplan.DemandSource{
		"plan-orders": capacityplan.DemandSourceOrders, "plan-request": capacityplan.DemandSourceRequest, "plan-default": capacityplan.DemandSourceRequest,
	}
	for id, w := range want {
		got, err := repo.FindByID(ctx, id)
		if err != nil || got == nil || got.DemandSource() != w {
			t.Fatalf("%s: source = %v, err %v, want %s", id, got, err, w)
		}
	}

	// A row written by the pre-0006 INSERT (no demand_source column) takes the default.
	if _, err := pool.Exec(ctx, `
		INSERT INTO capacity_plans (id, warehouse_id, location, window_start, window_end, process_path_id,
			assigned_demand, path_capacity, bottleneck_step, capacity_over_window, shortage, status, created_at)
		VALUES ('legacy', 'WH-1', 'SIM1', $1, $2, 'pick', 10, 1000, 'PICK', 8000, 0, 'DRAFT', $3)`, odStart, odEnd, odAsOf); err != nil {
		t.Fatal(err)
	}
	legacy, err := repo.FindByID(ctx, "legacy")
	if err != nil || legacy == nil || legacy.DemandSource() != capacityplan.DemandSourceRequest {
		t.Fatalf("legacy plan = %+v, %v; want source request", legacy, err)
	}
}
