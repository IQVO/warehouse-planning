//go:build integration

package postgres_test

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/golang-migrate/migrate/v4"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/claudioed/warehouse-planning/internal/adapters/outbound/postgres"
	"github.com/claudioed/warehouse-planning/internal/application/ports"
	"github.com/claudioed/warehouse-planning/internal/domain/capacityplan"
)

// docs/adr/0024 decision 3 on Postgres: migration 0017 (backfill, checks,
// the partial unique index, reversible) and publishes of one key that race.

const typeSuperseded = "com.warehouse.wes.warehouse-planning.capacityplan.CapacityPlanSuperseded"

// TestMigration0017_BackfillChecksAndUpDownUp loads PUBLISHED plans the way
// a deployed database may hold them (several per key) at version 0016, then
// migrates: every PUBLISHED plan but the newest of its key (published_at,
// then id) is SUPERSEDED by that newest one, nothing is queued, and the
// index and checks hold from then on. Down to 0016 a superseded plan reads
// back as PUBLISHED; up again backfills the same way.
func TestMigration0017_BackfillChecksAndUpDownUp(t *testing.T) {
	ctx := context.Background()
	databaseURL := startPostgres(t)
	m, err := migrate.New("file://"+migrationsDir(t), databaseURL)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _, _ = m.Close() }()
	if err := m.Migrate(16); err != nil {
		t.Fatalf("up to 0016: %v", err)
	}
	pool, err := postgres.NewPool(ctx, databaseURL)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()

	t1, t2 := planStart.Add(-3*time.Hour), planStart.Add(-2*time.Hour)
	insert := func(id, site, path, status string, publishedAt *time.Time) {
		t.Helper()
		if _, err := pool.Exec(ctx, `
			INSERT INTO capacity_plans (id, warehouse_id, site_id, location, window_start, window_end, process_path_id,
				assigned_demand, path_capacity, bottleneck_step, capacity_over_window, shortage, status, created_at, published_at)
			VALUES ($1, 'WH-1', $2, 'SIM1', $3, $4, $5, 10, 1, 'PICK', 8, 2, $6, $7, $8)`,
			id, site, planStart, planEnd, path, status, t1.Add(-time.Hour), publishedAt); err != nil {
			t.Fatalf("insert %s: %v", id, err)
		}
	}
	insert("k-p1", "SIM1", "pick-only", "PUBLISHED", &t1)
	insert("k-p2", "SIM1", "pick-only", "PUBLISHED", &t2) // ties with k-p3 on published_at: the id breaks it
	insert("k-p3", "SIM1", "pick-only", "PUBLISHED", &t2)
	insert("k-draft", "SIM1", "pick-only", "DRAFT", nil)
	insert("other-path", "SIM1", "pick-again", "PUBLISHED", &t1)
	insert("legacy-a", "", "pick-only", "PUBLISHED", &t1) // a pre-0008 site '' is its own key
	insert("legacy-b", "", "pick-only", "PUBLISHED", &t2)

	if err := m.Migrate(17); err != nil {
		t.Fatalf("up to 0017: %v", err)
	}
	wantBackfill := map[string][2]string{
		"k-p1": {"SUPERSEDED", "k-p3"}, "k-p2": {"SUPERSEDED", "k-p3"}, "k-p3": {"PUBLISHED", ""},
		"k-draft": {"DRAFT", ""}, "other-path": {"PUBLISHED", ""},
		"legacy-a": {"SUPERSEDED", "legacy-b"}, "legacy-b": {"PUBLISHED", ""},
	}
	assertStatuses(t, pool, wantBackfill)
	if n := count(t, pool, `SELECT count(*) FROM outbox_events`); n != 0 {
		t.Errorf("the backfill queued %d outbox rows, want none", n)
	}
	stored, err := postgres.NewCapacityPlanRepo(pool).FindByID(ctx, "k-p1")
	if err != nil || stored.Status() != capacityplan.StatusSuperseded || stored.SupersededBy() != "k-p3" || !stored.PublishedAt().Equal(t1) {
		t.Fatalf("k-p1 reads back as %+v, %v; want SUPERSEDED by k-p3, published_at kept", stored, err)
	}

	for name, q := range map[string]string{
		"a second PUBLISHED plan of a key":  `UPDATE capacity_plans SET status = 'PUBLISHED', superseded_by = NULL WHERE id = 'k-p1'`,
		"SUPERSEDED without superseded_by":  `UPDATE capacity_plans SET superseded_by = NULL WHERE id = 'k-p2'`,
		"superseded_by on a PUBLISHED plan": `UPDATE capacity_plans SET superseded_by = 'k-p1' WHERE id = 'k-p3'`,
		"a blank superseded_by":             `UPDATE capacity_plans SET superseded_by = '' WHERE id = 'k-p2'`,
		"an unknown status":                 `UPDATE capacity_plans SET status = 'RETIRED' WHERE id = 'k-draft'`,
	} {
		if _, err := pool.Exec(ctx, q); err == nil {
			t.Errorf("%s: the table accepted a state the domain never writes", name)
		}
	}

	if err := m.Migrate(16); err != nil {
		t.Fatalf("down 0017: %v", err)
	}
	if n := count(t, pool, `SELECT count(*) FROM capacity_plans WHERE status = 'PUBLISHED'`); n != 6 {
		t.Errorf("down 0017: %d PUBLISHED plans, want 6 (every superseded plan reads back as PUBLISHED)", n)
	}
	if _, err := pool.Exec(ctx, `UPDATE capacity_plans SET status = 'SUPERSEDED' WHERE id = 'k-draft'`); err == nil {
		t.Error("down 0017 kept SUPERSEDED in the status CHECK")
	}
	if err := m.Up(); err != nil {
		t.Fatalf("up again: %v", err)
	}
	assertStatuses(t, pool, wantBackfill)
}

// assertStatuses checks each plan's stored status and superseded_by.
func assertStatuses(t *testing.T, pool *pgxpool.Pool, want map[string][2]string) {
	t.Helper()
	for id, w := range want {
		var status, by string
		if err := pool.QueryRow(context.Background(),
			`SELECT status, COALESCE(superseded_by, '') FROM capacity_plans WHERE id = $1`, id).Scan(&status, &by); err != nil {
			t.Fatalf("read %s: %v", id, err)
		}
		if status != w[0] || by != w[1] {
			t.Errorf("%s = %s by %q, want %s by %q", id, status, by, w[0], w[1])
		}
	}
}

// pausesAfterFindPublished holds the FIRST publish attempt open right after
// it looked up the plan to supersede, so another publish provably overlaps.
type pausesAfterFindPublished struct {
	ports.CapacityPlanRepository
	once    *sync.Once
	found   chan struct{}
	release chan struct{}
}

func (p pausesAfterFindPublished) FindPublished(ctx context.Context, key capacityplan.SupersedeKey, excludeID string) (*capacityplan.CapacityPlan, error) {
	plan, err := p.CapacityPlanRepository.FindPublished(ctx, key, excludeID)
	p.once.Do(func() {
		close(p.found)
		<-p.release
	})
	return plan, err
}

func newPause(plans ports.CapacityPlanRepository) pausesAfterFindPublished {
	return pausesAfterFindPublished{CapacityPlanRepository: plans, once: &sync.Once{}, found: make(chan struct{}), release: make(chan struct{})}
}

// racePublishes runs publish A, held after its FindPublished, and publish B
// meanwhile, then releases A; both must succeed.
func racePublishes(t *testing.T, s *planStack, a, b string) {
	t.Helper()
	ctx := context.Background()
	pause := newPause(s.plans)
	publishA := *s.publish
	publishA.Plans = pause

	var wg sync.WaitGroup
	var errA, errB error
	wg.Add(1)
	go func() { defer wg.Done(); _, errA = publishA.Handle(ctx, a) }()
	<-pause.found
	wg.Add(1)
	go func() { defer wg.Done(); _, errB = s.publish.Handle(ctx, b) }()
	time.Sleep(500 * time.Millisecond) // B is now blocked on A's row lock, or has committed
	close(pause.release)
	wg.Wait()
	if errA != nil || errB != nil {
		t.Fatalf("publish A err = %v, publish B err = %v; want both to succeed", errA, errB)
	}
}

// Two DRAFTs of one key published at once, with no published plan yet: both
// see none, B commits first, A loses on the partial unique index
// (ErrVersionConflict), reruns its unit and supersedes B.
func TestPublish_Postgres_RacingFirstPublishesOfOneKey(t *testing.T) {
	s := newPlanStack(t)
	ctx := context.Background()
	a, err := s.create.Handle(ctx, planCmd(6000))
	if err != nil {
		t.Fatal(err)
	}
	b, err := s.create.Handle(ctx, planCmd(6000))
	if err != nil {
		t.Fatal(err)
	}
	racePublishes(t, s, a.ID(), b.ID())

	assertStatuses(t, s.pool, map[string][2]string{a.ID(): {"PUBLISHED", ""}, b.ID(): {"SUPERSEDED", a.ID()}})
	if got := eventTypes(t, s.pool); !equalStrings(got, []string{typeCreated, typeCreated, typePublished, typeSuperseded, typePublished}) {
		t.Fatalf("outbox = %v, want B's publication, then A's supersede of B and A's publication", got)
	}
}

// O is PUBLISHED; A and B are published at once. A locks O (FOR UPDATE) and
// is held; B's lookup waits on that lock, then no longer sees O as
// PUBLISHED, loses on the index, reruns and supersedes A. O is superseded
// exactly once.
func TestPublish_Postgres_RacingPublishesNeverSupersedeAPlanTwice(t *testing.T) {
	s := newPlanStack(t)
	ctx := context.Background()
	var ids []string
	for range 3 {
		p, err := s.create.Handle(ctx, planCmd(6000))
		if err != nil {
			t.Fatal(err)
		}
		ids = append(ids, p.ID())
	}
	o, a, b := ids[0], ids[1], ids[2]
	if _, err := s.publish.Handle(ctx, o); err != nil {
		t.Fatal(err)
	}
	racePublishes(t, s, a, b)

	assertStatuses(t, s.pool, map[string][2]string{o: {"SUPERSEDED", a}, a: {"SUPERSEDED", b}, b: {"PUBLISHED", ""}})
	if n := count(t, s.pool, `SELECT count(*) FROM outbox_events WHERE event_type = $1 AND subject = $2`, typeSuperseded, o); n != 1 {
		t.Errorf("O superseded %d times, want once", n)
	}
	if n := count(t, s.pool, `SELECT count(*) FROM outbox_events WHERE event_type = $1`, typeSuperseded); n != 2 {
		t.Errorf("%d CapacityPlanSuperseded rows, want 2 (O by A, A by B)", n)
	}
}
