//go:build integration

package postgres_test

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/golang-migrate/migrate/v4"

	"github.com/claudioed/warehouse-planning/internal/adapters/outbound/memory"
	"github.com/claudioed/warehouse-planning/internal/adapters/outbound/postgres"
	"github.com/claudioed/warehouse-planning/internal/domain/capacityplan"
	"github.com/claudioed/warehouse-planning/internal/domain/inbound"
	"github.com/claudioed/warehouse-planning/internal/domain/observed"
	"github.com/claudioed/warehouse-planning/internal/domain/processcapacity"
)

var (
	ibStart = time.Date(2026, 10, 9, 8, 0, 0, 0, time.UTC)
	ibEnd   = time.Date(2026, 10, 9, 16, 0, 0, 0, time.UTC)
	ibAsOf  = time.Date(2026, 10, 8, 12, 0, 0, 0, time.UTC)
)

func ibBooked(t *testing.T, id, location string, start time.Time, asns int, asOf time.Time) inbound.Appointment {
	t.Helper()
	a, err := inbound.NewBooked(inbound.BookedParams{AppointmentID: id, Location: location, WindowStart: start, WindowEnd: start.Add(2 * time.Hour), ASNCount: asns, AsOf: asOf})
	if err != nil {
		t.Fatal(err)
	}
	return a
}

func ibCancelled(t *testing.T, id, location string, asOf time.Time) inbound.Appointment {
	t.Helper()
	a, err := inbound.NewCancelled(id, location, asOf)
	if err != nil {
		t.Fatal(err)
	}
	return a
}

// The SQL window predicate must agree with inbound.Appointment.CountsIn at every
// boundary: a door window starting at `start` counts, one starting at `end` does
// not, a cancelled appointment and a tombstone never count.
func TestInboundAppointmentRepo_DemandWindowBoundariesMatchTheDomainRule(t *testing.T) {
	pool := newMigratedPool(t)
	repo := postgres.NewInboundAppointmentRepo(pool)
	mem := memory.NewInboundAppointmentRepo()
	ctx := context.Background()

	events := []inbound.Appointment{
		ibBooked(t, "before", "SIM1", ibStart.Add(-time.Second), 1, ibAsOf),
		ibBooked(t, "at-start", "SIM1", ibStart, 2, ibAsOf.Add(time.Minute)),
		ibBooked(t, "last-second", "SIM1", ibEnd.Add(-time.Second), 3, ibAsOf.Add(2*time.Minute)),
		ibBooked(t, "at-end", "SIM1", ibEnd, 5, ibAsOf.Add(time.Hour)),
		ibBooked(t, "other-site", "SIM2", ibStart.Add(time.Hour), 7, ibAsOf.Add(2*time.Hour)),
		ibBooked(t, "cancelled", "SIM1", ibStart.Add(time.Hour), 11, ibAsOf),
		ibCancelled(t, "cancelled", "SIM1", ibAsOf.Add(3*time.Minute)),
		ibCancelled(t, "tombstone", "SIM1", ibAsOf.Add(4*time.Minute)),
	}
	for _, e := range events {
		pg, err := repo.Apply(ctx, e)
		if err != nil {
			t.Fatalf("Apply(%s): %v", e.ID(), err)
		}
		m, _ := mem.Apply(ctx, e)
		if pg != m {
			t.Fatalf("Apply(%s) changed: postgres %v, in-memory %v", e.ID(), pg, m)
		}
	}

	got, err := repo.Demand(ctx, "SIM1", ibStart, ibEnd)
	if err != nil {
		t.Fatal(err)
	}
	if got.Appointments != 2 || got.ASNs != 2+3 {
		t.Fatalf("demand = %+v, want the appointments at start and one second before end", got)
	}
	if !got.AsOf.Equal(ibAsOf.Add(time.Hour)) {
		t.Fatalf("AsOf = %v, want the site's newest event, not the other site's", got.AsOf)
	}
	want, _ := mem.Demand(ctx, "SIM1", ibStart, ibEnd)
	if got != want {
		t.Fatalf("postgres %+v != in-memory %+v: the two adapters disagree", got, want)
	}
	if empty, err := repo.Demand(ctx, "NOWHERE", ibStart, ibEnd); err != nil || empty.Appointments != 0 || !empty.AsOf.IsZero() {
		t.Fatalf("unknown site = %+v, %v; want the zero Demand", empty, err)
	}
}

// The ordering guard must be the same function in both adapters, in EVERY
// arrival order of a Booked and its Cancelled.
func TestInboundAppointmentRepo_OrderingGuardLikeTheInMemoryAdapter(t *testing.T) {
	pool := newMigratedPool(t)
	pg := postgres.NewInboundAppointmentRepo(pool)
	mem := memory.NewInboundAppointmentRepo()
	ctx := context.Background()

	steps := []struct {
		name        string
		event       inbound.Appointment
		wantChanged bool
	}{
		{"cancelled arrives before its booked: tombstone", ibCancelled(t, "a1", "SIM1", ibAsOf.Add(time.Hour)), true},
		{"the older booked is stale", ibBooked(t, "a1", "SIM1", ibStart, 2, ibAsOf), false},
		{"a redelivered cancel changes nothing", ibCancelled(t, "a1", "SIM1", ibAsOf.Add(time.Hour)), false},
		{"normal order: booked", ibBooked(t, "a2", "SIM1", ibStart, 2, ibAsOf), true},
		{"normal order: cancelled later wins", ibCancelled(t, "a2", "SIM1", ibAsOf.Add(time.Minute)), true},
		{"same-time cancel wins the tie", ibCancelled(t, "a3", "SIM1", ibAsOf), true},
		{"same-time booked does not undo a cancel", ibBooked(t, "a3", "SIM1", ibStart, 1, ibAsOf), false},
		{"booked then same-time cancel", ibBooked(t, "a4", "SIM1", ibStart, 1, ibAsOf), true},
		{"the tie goes to the cancel", ibCancelled(t, "a4", "SIM1", ibAsOf), true},
	}
	for _, s := range steps {
		gotPg, err := pg.Apply(ctx, s.event)
		if err != nil {
			t.Fatalf("%s: %v", s.name, err)
		}
		gotMem, _ := mem.Apply(ctx, s.event)
		if gotPg != s.wantChanged || gotMem != s.wantChanged {
			t.Fatalf("%s: changed pg=%v mem=%v, want %v", s.name, gotPg, gotMem, s.wantChanged)
		}
	}
	var rows int
	if err := pool.QueryRow(ctx, "SELECT count(*) FROM inbound_appointments").Scan(&rows); err != nil || rows != 4 {
		t.Fatalf("rows = %d, %v; one appointment id must stay one row", rows, err)
	}
	var status string
	if err := pool.QueryRow(ctx, "SELECT status FROM inbound_appointments WHERE appointment_id = 'a1'").Scan(&status); err != nil || status != "CANCELLED" {
		t.Fatalf("a1 status = %q, %v; a Cancelled before its Booked must win", status, err)
	}
	// A cancellation keeps the booking's window and ASN count.
	var asns int
	if err := pool.QueryRow(ctx, "SELECT asn_count FROM inbound_appointments WHERE appointment_id = 'a2'").Scan(&asns); err != nil || asns != 2 {
		t.Fatalf("a2 asn_count = %d, %v; the cancelled row must keep what was cancelled", asns, err)
	}
	for _, w := range [][2]time.Time{{ibStart, ibEnd}, {ibEnd, ibEnd.Add(8 * time.Hour)}} {
		a, _ := pg.Demand(ctx, "SIM1", w[0], w[1])
		b, _ := mem.Demand(ctx, "SIM1", w[0], w[1])
		if a != b {
			t.Fatalf("window %v: postgres %+v != in-memory %+v", w, a, b)
		}
	}
}

// A Booked and a Cancelled applied CONCURRENTLY, with no stored row, must both
// be serialized by the advisory lock and end CANCELLED.
func TestInboundAppointmentRepo_ConcurrentFirstWritesConvergeOnCancelled(t *testing.T) {
	pool := newMigratedPool(t)
	repo := postgres.NewInboundAppointmentRepo(pool)
	ctx := context.Background()
	for i := 0; i < 20; i++ {
		id := fmt.Sprintf("race-%d", i)
		book := ibBooked(t, id, "SIM1", ibStart, 1, ibAsOf)
		cancel := ibCancelled(t, id, "SIM1", ibAsOf.Add(time.Minute))
		var wg sync.WaitGroup
		for _, e := range []inbound.Appointment{book, cancel} {
			wg.Add(1)
			go func() {
				defer wg.Done()
				if _, err := repo.Apply(ctx, e); err != nil {
					t.Errorf("Apply(%s): %v", e.ID(), err)
				}
			}()
		}
		wg.Wait()
		var status string
		if err := pool.QueryRow(ctx, "SELECT status FROM inbound_appointments WHERE appointment_id = $1", id).Scan(&status); err != nil || status != "CANCELLED" {
			t.Fatalf("%s: status = %q, %v; the later Cancelled must win whatever the arrival order", id, status, err)
		}
	}
}

// The consumer's claim and Apply are ONE transaction.
func TestInboundAppointmentRepo_ClaimAndApplyCommitOrRollBackTogether(t *testing.T) {
	pool := newMigratedPool(t)
	uow := postgres.NewUnitOfWork(pool)
	repo := postgres.NewInboundAppointmentRepo(pool)
	processed := postgres.NewProcessedEventRepo(pool)
	ctx := context.Background()
	a := ibBooked(t, "a1", "SIM1", ibStart, 1, ibAsOf)
	boom := errors.New("injected failure after the write")

	err := uow.Do(ctx, func(ctx context.Context) error {
		if claimed, err := processed.Claim(ctx, "inbound-demand-consumer", "evt-1"); err != nil || !claimed {
			return fmt.Errorf("claim = %v, %v", claimed, err)
		}
		if _, err := repo.Apply(ctx, a); err != nil {
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
	if count("SELECT count(*) FROM inbound_appointments") != 0 || count("SELECT count(*) FROM processed_events WHERE consumer='inbound-demand-consumer'") != 0 {
		t.Fatal("a failed unit of work left an appointment or a claim behind")
	}
	if err := uow.Do(ctx, func(ctx context.Context) error {
		if _, err := processed.Claim(ctx, "inbound-demand-consumer", "evt-1"); err != nil {
			return err
		}
		_, err := repo.Apply(ctx, a)
		return err
	}); err != nil {
		t.Fatal(err)
	}
	if count("SELECT count(*) FROM inbound_appointments") != 1 || count("SELECT count(*) FROM processed_events WHERE consumer='inbound-demand-consumer'") != 1 {
		t.Fatal("a successful unit of work did not commit both rows")
	}
}

func TestInboundStandardRepo_SaveFindListLikeTheInMemoryAdapter(t *testing.T) {
	pool := newMigratedPool(t)
	pg := postgres.NewInboundStandardRepo(pool)
	mem := memory.NewInboundStandardRepo()
	ctx := context.Background()
	mk := func(loc, path string, per, perASN float64) inbound.LabourStandard {
		s, err := inbound.NewLabourStandard(loc, path, per, perASN)
		if err != nil {
			t.Fatal(err)
		}
		return s
	}
	for _, s := range []inbound.LabourStandard{
		mk("SIM2", "receive", 20, 0), mk("SIM1", "receive", 30, 5), mk("SIM1", "putaway", 10, 0), mk("SIM1", "receive", 45, 2.5), // the last REPLACES
	} {
		if err := pg.Save(ctx, s); err != nil {
			t.Fatal(err)
		}
		_ = mem.Save(ctx, s)
	}
	got, err := pg.Find(ctx, "SIM1", "receive")
	if err != nil || got == nil || got.MinutesPerAppointment() != 45 || got.MinutesPerASN() != 2.5 {
		t.Fatalf("Find = %+v, %v; want the replacement", got, err)
	}
	if none, err := pg.Find(ctx, "SIM1", "nope"); err != nil || none != nil {
		t.Fatalf("Find(unknown) = %+v, %v; want (nil, nil)", none, err)
	}
	for _, loc := range []string{"", "SIM1", "NOWHERE"} {
		a, err := pg.List(ctx, loc)
		if err != nil {
			t.Fatal(err)
		}
		b, _ := mem.List(ctx, loc)
		if len(a) != len(b) {
			t.Fatalf("List(%q): postgres %d != in-memory %d", loc, len(a), len(b))
		}
		for i := range a {
			if a[i] != b[i] {
				t.Fatalf("List(%q)[%d]: postgres %+v != in-memory %+v", loc, i, a[i], b[i])
			}
		}
	}
}

func TestCapacityPlanRepo_PersistsInboundDemandAndLegacyRowsReadAsNone(t *testing.T) {
	pool := newMigratedPool(t)
	repo := postgres.NewCapacityPlanRepo(pool)
	ctx := context.Background()
	window, err := processcapacity.NewCapacityWindow(ibStart, ibEnd)
	if err != nil {
		t.Fatal(err)
	}
	rate, err := processcapacity.NewCapacityRate(1000, processcapacity.UnitOrder, time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	in := capacityplan.InboundDemand{Appointments: 3, LabourMinutes: 142.5, StandardMissing: true}
	p, err := capacityplan.Create(capacityplan.CreateParams{
		ID: "plan-inbound", WarehouseID: "WH-1", SiteID: "SIM1", Location: "SIM1", Window: window, ProcessPathID: "receive",
		AssignedDemand: 10, PathRate: rate, BottleneckStep: "RECEIVE", Inbound: in,
	}, ibAsOf)
	if err != nil {
		t.Fatal(err)
	}
	if err := repo.Save(ctx, p); err != nil {
		t.Fatal(err)
	}
	got, err := repo.FindByID(ctx, "plan-inbound")
	if err != nil || got == nil || got.Inbound() != in {
		t.Fatalf("plan = %+v, %v; want inbound %+v", got, err, in)
	}
	// Publishing re-saves the plan: it must not lose the inbound figures.
	if err := got.Publish(ibAsOf.Add(time.Hour)); err != nil {
		t.Fatal(err)
	}
	if err := repo.Save(ctx, got); err != nil {
		t.Fatal(err)
	}
	if again, _ := repo.FindByID(ctx, "plan-inbound"); again == nil || again.Inbound() != in {
		t.Fatalf("publish changed the inbound figures: %+v", again)
	}

	if _, err := pool.Exec(ctx, `
		INSERT INTO capacity_plans (id, warehouse_id, location, window_start, window_end, process_path_id,
			assigned_demand, path_capacity, bottleneck_step, capacity_over_window, shortage, status, created_at)
		VALUES ('legacy', 'WH-1', 'SIM1', $1, $2, 'pick', 10, 1000, 'PICK', 8000, 0, 'DRAFT', $3)`, ibStart, ibEnd, ibAsOf); err != nil {
		t.Fatal(err)
	}
	legacy, err := repo.FindByID(ctx, "legacy")
	if err != nil || legacy == nil || legacy.Inbound() != (capacityplan.InboundDemand{}) {
		t.Fatalf("legacy plan = %+v, %v; want no inbound demand", legacy, err)
	}
}

// --- observed capacity -------------------------------------------------------

var obStart = time.Date(2026, 9, 30, 14, 0, 0, 0, time.UTC)

func obCompletion(t *testing.T, location, step string, at time.Time, dur *int64) observed.Completion {
	t.Helper()
	c, err := observed.NewCompletion(observed.CompletionParams{Location: location, ProcessType: step, At: at, DurationSeconds: dur})
	if err != nil {
		t.Fatal(err)
	}
	return c
}

func i64(v int64) *int64 { return &v }

func TestObservedCapacityRepo_ThroughputBoundariesMatchTheDomainRuleAndTheInMemoryAdapter(t *testing.T) {
	pool := newMigratedPool(t)
	pg := postgres.NewObservedCapacityRepo(pool)
	mem := memory.NewObservedCapacityRepo()
	ctx := context.Background()
	end := obStart.Add(2 * time.Hour)

	for _, c := range []observed.Completion{
		obCompletion(t, "SIM1", "PICK", obStart.Add(-time.Second), i64(10)),   // previous minute: out
		obCompletion(t, "SIM1", "PICK", obStart, i64(20)),                     // at start: in
		obCompletion(t, "SIM1", "PICK", obStart.Add(30*time.Second), i64(40)), // same bucket: in
		obCompletion(t, "SIM1", "PICK", end.Add(-time.Second), nil),           // last second: in, no duration
		obCompletion(t, "SIM1", "PICK", end, i64(80)),                         // at end: out
		obCompletion(t, "SIM1", "PACK", obStart.Add(time.Hour), i64(5)),       // another step
		obCompletion(t, "SIM2", "PICK", obStart.Add(time.Hour), i64(5)),       // another site
	} {
		if err := pg.Record(ctx, c); err != nil {
			t.Fatal(err)
		}
		_ = mem.Record(ctx, c)
	}
	got, err := pg.Throughput(ctx, "SIM1", "PICK", obStart, end)
	if err != nil {
		t.Fatal(err)
	}
	if got.Tasks != 3 || got.DurationSeconds != 60 || got.TasksWithDuration != 2 {
		t.Fatalf("throughput = %+v", got)
	}
	if !got.AsOf.Equal(end) {
		t.Fatalf("AsOf = %v, want the step's newest completion (the one at the window end)", got.AsOf)
	}
	want, _ := mem.Throughput(ctx, "SIM1", "PICK", obStart, end)
	if got != want {
		t.Fatalf("postgres %+v != in-memory %+v: the two adapters disagree", got, want)
	}
	if empty, err := pg.Throughput(ctx, "SIM1", "SLAM", obStart, end); err != nil || empty.HasData() || !empty.AsOf.IsZero() {
		t.Fatalf("unobserved step = %+v, %v; want the zero Throughput", empty, err)
	}
}

// Two writers of ONE minute bucket must both count: the increment is in SQL.
func TestObservedCapacityRepo_ConcurrentWritersOfOneBucketAllCount(t *testing.T) {
	pool := newMigratedPool(t)
	repo := postgres.NewObservedCapacityRepo(pool)
	ctx := context.Background()
	const writers = 25
	var wg sync.WaitGroup
	for i := 0; i < writers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := repo.Record(ctx, obCompletion(t, "SIM1", "PICK", obStart.Add(time.Duration(i)*time.Second), i64(2))); err != nil {
				t.Errorf("Record: %v", err)
			}
		}()
	}
	wg.Wait()
	got, err := repo.Throughput(ctx, "SIM1", "PICK", obStart, obStart.Add(time.Hour))
	if err != nil || got.Tasks != writers || got.DurationSeconds != 2*writers {
		t.Fatalf("throughput = %+v, %v; want %d tasks", got, err, writers)
	}
	var buckets int
	if err := pool.QueryRow(ctx, "SELECT count(*) FROM observed_capacity").Scan(&buckets); err != nil || buckets != 1 {
		t.Fatalf("buckets = %d, %v; the 25 completions of one minute are ONE row", buckets, err)
	}
}

func TestObservedCapacityRepo_ClaimAndRecordCommitOrRollBackTogether(t *testing.T) {
	pool := newMigratedPool(t)
	uow := postgres.NewUnitOfWork(pool)
	repo := postgres.NewObservedCapacityRepo(pool)
	processed := postgres.NewProcessedEventRepo(pool)
	ctx := context.Background()
	c := obCompletion(t, "SIM1", "PICK", obStart, i64(9))
	boom := errors.New("injected failure after the write")

	err := uow.Do(ctx, func(ctx context.Context) error {
		if claimed, err := processed.Claim(ctx, "observed-capacity-consumer", "evt-1"); err != nil || !claimed {
			return fmt.Errorf("claim = %v, %v", claimed, err)
		}
		if err := repo.Record(ctx, c); err != nil {
			return err
		}
		return boom
	})
	if !errors.Is(err, boom) {
		t.Fatalf("err = %v", err)
	}
	var buckets, claims int
	_ = pool.QueryRow(ctx, "SELECT count(*) FROM observed_capacity").Scan(&buckets)
	_ = pool.QueryRow(ctx, "SELECT count(*) FROM processed_events WHERE consumer='observed-capacity-consumer'").Scan(&claims)
	if buckets != 0 || claims != 0 {
		t.Fatalf("a failed unit of work left %d buckets / %d claims behind", buckets, claims)
	}
}

func TestMigrations0009And0010_DownIsCleanAndUpIsRepeatable(t *testing.T) {
	databaseURL := startPostgres(t)
	if err := postgres.RunMigrations(databaseURL, migrationsDir(t)); err != nil {
		t.Fatal(err)
	}
	m, err := migrate.New("file://"+migrationsDir(t), databaseURL)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _, _ = m.Close() }()
	pool, err := postgres.NewPool(context.Background(), databaseURL)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	exists := func(query string) int {
		var n int
		if err := pool.QueryRow(context.Background(), query).Scan(&n); err != nil {
			t.Fatal(err)
		}
		return n
	}
	tables := func(name string) int {
		return exists("SELECT count(*) FROM information_schema.tables WHERE table_name = '" + name + "'")
	}
	planCols := func() int {
		return exists("SELECT count(*) FROM information_schema.columns WHERE table_name = 'capacity_plans' AND column_name LIKE 'inbound_%'")
	}

	if tables("observed_capacity") != 1 || tables("inbound_appointments") != 1 || tables("inbound_labour_standards") != 1 || planCols() != 3 {
		t.Fatal("the up migrations did not create the 0009/0010 objects")
	}
	if err := m.Steps(-1); err != nil {
		t.Fatalf("down 0010: %v", err)
	}
	if tables("observed_capacity") != 0 || tables("inbound_appointments") != 1 {
		t.Fatal("down 0010 must drop only observed_capacity")
	}
	if err := m.Steps(-1); err != nil {
		t.Fatalf("down 0009: %v", err)
	}
	if tables("inbound_appointments") != 0 || tables("inbound_labour_standards") != 0 || planCols() != 0 {
		t.Fatal("down 0009 left inbound tables or capacity_plans columns behind")
	}
	if err := m.Up(); err != nil {
		t.Fatalf("up again: %v", err)
	}
	if tables("observed_capacity") != 1 || tables("inbound_appointments") != 1 || planCols() != 3 {
		t.Fatal("up again did not recreate the objects")
	}
}
