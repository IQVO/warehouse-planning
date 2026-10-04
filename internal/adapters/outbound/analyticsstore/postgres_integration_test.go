//go:build integration

package analyticsstore_test

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/testcontainers/testcontainers-go"
	tcpostgres "github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/wait"

	"github.com/claudioed/warehouse-planning/internal/adapters/outbound/analyticsstore"
	"github.com/claudioed/warehouse-planning/internal/adapters/outbound/postgres"
	"github.com/claudioed/warehouse-planning/internal/analytics/report"
)

// One real Postgres (testcontainers, never skip-gated) serves the package;
// every test starts from truncated tables.
var (
	pgOnce      sync.Once
	pgURL       string
	pgContainer testcontainers.Container
	pgErr       error
)

func TestMain(m *testing.M) {
	code := m.Run()
	if pgContainer != nil {
		if err := testcontainers.TerminateContainer(pgContainer); err != nil {
			fmt.Fprintf(os.Stderr, "terminate postgres container: %v\n", err)
		}
	}
	os.Exit(code)
}

func analyticsMigrationsDir(t *testing.T) string {
	t.Helper()
	_, thisFile, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("unable to resolve test file path")
	}
	return filepath.Join(filepath.Dir(thisFile), "..", "..", "..", "..", "analytics", "migrations")
}

// analyticalDB boots the analytical database once, applies the analytics
// migrations over the direct DSN, and returns its DSN.
func analyticalDB(t *testing.T) string {
	t.Helper()
	pgOnce.Do(func() {
		ctx := context.Background()
		c, err := tcpostgres.Run(ctx, "postgres:16-alpine",
			tcpostgres.WithDatabase("warehouse_planning_analytics"),
			tcpostgres.WithUsername("projector"),
			tcpostgres.WithPassword("projector"),
			testcontainers.WithWaitStrategy(
				wait.ForLog("database system is ready to accept connections").WithOccurrence(2).WithStartupTimeout(60*time.Second)),
		)
		if err != nil {
			pgErr = err
			return
		}
		pgContainer = c
		if pgURL, pgErr = c.ConnectionString(ctx, "sslmode=disable"); pgErr != nil {
			return
		}
		pgErr = postgres.RunMigrations(pgURL, analyticsMigrationsDir(t))
	})
	if pgErr != nil {
		t.Fatalf("analytical database: %v", pgErr)
	}
	return pgURL
}

// freshStores returns the writer and the (read-only) reader over truncated
// tables.
func freshStores(t *testing.T) (*analyticsstore.Projection, *analyticsstore.Reader, *pgxpool.Pool) {
	t.Helper()
	url := analyticalDB(t)
	ctx := context.Background()
	w, err := analyticsstore.NewPool(ctx, url)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(w.Close)
	if _, err := w.Exec(ctx, `TRUNCATE plan_facts, analytics_processed_events`); err != nil {
		t.Fatal(err)
	}
	ro, err := analyticsstore.NewReadOnlyPool(ctx, url)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(ro.Close)
	return analyticsstore.NewProjection(w), analyticsstore.NewReader(ro), w
}

func TestPostgresStore_SatisfiesTheStoreContract(t *testing.T) {
	runStoreContract(t, func(t *testing.T) (report.Projection, report.Reader) {
		p, r, _ := freshStores(t)
		return p, r
	})
}

func count(t *testing.T, pool *pgxpool.Pool, table string) int {
	t.Helper()
	var n int
	if err := pool.QueryRow(context.Background(), `SELECT count(*) FROM `+table).Scan(&n); err != nil {
		t.Fatal(err)
	}
	return n
}

func publishedEvent(id string) report.PlanEvent {
	return report.PlanEvent{
		Kind: report.KindPublished, EventID: id, At: at(5, 8, 0, 0), PlanID: "plan-" + id, WarehouseID: "WH-1", Location: "SIM1",
		BottleneckStep: ptr("PACK"), BindingConstraint: ptr("STATION"), Shortage: ptr(5600.0), PublishedAt: ptr(at(5, 8, 0, 0)),
	}
}

// The projection write and the processed-id mark are ONE transaction: when
// the plan upsert fails the mark is rolled back with it, and the redelivered
// event applies cleanly once the failure is gone.
func TestPostgresStore_ATransientFailureLeavesNothingWrittenAndTheMarkUnrecorded(t *testing.T) {
	p, _, pool := freshStores(t)
	ctx := context.Background()
	for _, stmt := range []string{
		`CREATE OR REPLACE FUNCTION fail_plan_facts() RETURNS trigger AS $$ BEGIN RAISE EXCEPTION 'simulated outage'; END $$ LANGUAGE plpgsql`,
		`CREATE TRIGGER fail_plan_facts BEFORE INSERT ON plan_facts FOR EACH ROW EXECUTE FUNCTION fail_plan_facts()`,
	} {
		if _, err := pool.Exec(ctx, stmt); err != nil {
			t.Fatal(err)
		}
	}
	t.Cleanup(func() { _, _ = pool.Exec(context.Background(), `DROP TRIGGER IF EXISTS fail_plan_facts ON plan_facts`) })

	applied, err := p.Apply(ctx, publishedEvent("evt-1"))
	if err == nil || applied {
		t.Fatalf("Apply = %v, %v; want an error", applied, err)
	}
	if errors.Is(err, report.ErrRejected) {
		t.Fatalf("a raised exception is transient infrastructure, not poison: %v", err)
	}
	if n := count(t, pool, "plan_facts"); n != 0 {
		t.Errorf("plan_facts rows = %d, want 0", n)
	}
	if n := count(t, pool, "analytics_processed_events"); n != 0 {
		t.Errorf("processed marks = %d, want 0 (the mark must roll back with the effect)", n)
	}

	if _, err := pool.Exec(ctx, `DROP TRIGGER fail_plan_facts ON plan_facts`); err != nil {
		t.Fatal(err)
	}
	if applied, err := p.Apply(ctx, publishedEvent("evt-1")); err != nil || !applied {
		t.Fatalf("redelivery = %v, %v; want applied", applied, err)
	}
	if count(t, pool, "plan_facts") != 1 || count(t, pool, "analytics_processed_events") != 1 {
		t.Fatal("the redelivered event must write the row and the mark")
	}
}

func TestPostgresStore_ADeterministicRejectionIsClassifiedAndWritesNothing(t *testing.T) {
	p, _, pool := freshStores(t)
	e := publishedEvent("evt-bad")
	e.Shortage = ptr(-1.0) // violates CHECK (shortage >= 0): integrity class 23
	applied, err := p.Apply(context.Background(), e)
	if applied || !errors.Is(err, report.ErrRejected) {
		t.Fatalf("Apply = %v, %v; want report.ErrRejected", applied, err)
	}
	if count(t, pool, "plan_facts") != 0 || count(t, pool, "analytics_processed_events") != 0 {
		t.Fatal("a rejected event must leave nothing behind, not even its mark")
	}
}

func TestPostgresStore_ConcurrentDeliveriesOfOneIdApplyExactlyOnce(t *testing.T) {
	p, _, pool := freshStores(t)
	var wg sync.WaitGroup
	results := make(chan bool, 12)
	for i := 0; i < 12; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			applied, err := p.Apply(context.Background(), publishedEvent("evt-race"))
			if err != nil {
				t.Errorf("Apply: %v", err)
			}
			results <- applied
		}()
	}
	wg.Wait()
	close(results)
	won := 0
	for ok := range results {
		if ok {
			won++
		}
	}
	if won != 1 || count(t, pool, "plan_facts") != 1 {
		t.Fatalf("applied %d times, plan rows %d; want exactly 1 and 1", won, count(t, pool, "plan_facts"))
	}
}

func TestPostgresStore_TheReportsPoolIsReadOnly(t *testing.T) {
	_, _, _ = freshStores(t)
	ro, err := analyticsstore.NewReadOnlyPool(context.Background(), analyticalDB(t))
	if err != nil {
		t.Fatal(err)
	}
	defer ro.Close()
	if _, err := ro.Exec(context.Background(), `INSERT INTO analytics_processed_events (event_id, event_type, occurred_at) VALUES ('x', 'y', now())`); err == nil {
		t.Fatal("the reports pool wrote to the analytical database")
	}
}

// Statement timeouts are applied to every connection of both pools.
func TestPostgresStore_PoolsApplyTheirStatementTimeouts(t *testing.T) {
	url := analyticalDB(t)
	ctx := context.Background()
	w, _ := analyticsstore.NewPool(ctx, url)
	ro, _ := analyticsstore.NewReadOnlyPool(ctx, url)
	defer w.Close()
	defer ro.Close()
	for pool, want := range map[*pgxpool.Pool]string{w: "10s", ro: "15s"} {
		var got string
		if err := pool.QueryRow(ctx, `SHOW statement_timeout`).Scan(&got); err != nil || got != want {
			t.Errorf("statement_timeout = %q, %v; want %q", got, err, want)
		}
	}
}

func TestAnalyticsMigration_DownThenUpIsClean(t *testing.T) {
	_, _, pool := freshStores(t)
	ctx := context.Background()
	down, err := os.ReadFile(filepath.Join(analyticsMigrationsDir(t), "0001_plan_facts.down.sql"))
	if err != nil {
		t.Fatal(err)
	}
	up, err := os.ReadFile(filepath.Join(analyticsMigrationsDir(t), "0001_plan_facts.up.sql"))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, string(down)); err != nil {
		t.Fatalf("down: %v", err)
	}
	var exists bool
	if err := pool.QueryRow(ctx, `SELECT to_regclass('plan_facts') IS NOT NULL`).Scan(&exists); err != nil || exists {
		t.Fatalf("plan_facts still exists after down (%v, %v)", exists, err)
	}
	if _, err := pool.Exec(ctx, string(up)); err != nil {
		t.Fatalf("up again: %v", err)
	}
}
