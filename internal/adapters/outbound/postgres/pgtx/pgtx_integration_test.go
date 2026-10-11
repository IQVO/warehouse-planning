//go:build integration

// Integration tests for the pgtx transaction-context mechanism against a
// REAL Postgres (testcontainers): pgtx.With/From is how postgres.UnitOfWork
// carries one open transaction through a context.Context so every
// repository joins the SAME atomic bracket without knowing about each
// other. These tests prove the all-or-nothing contract on the repo's main
// write flow — a CapacityPlan save plus its outbox insert commit together
// inside a UnitOfWork, and an error inside the scope rolls EVERYTHING back
// (neither the plan row nor any outbox row survives) — plus the
// context-propagation mechanics themselves (With/From round trip, nil-tx
// guard, nested join).
//
// One container for the whole package (TestMain below), migrated once into
// a template database; each test gets its own private database cloned from
// that template. Never an external DATABASE_URL, never t.Skip.
package pgtx_test

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"runtime"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/testcontainers/testcontainers-go"
	tcpostgres "github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/wait"

	"github.com/claudioed/warehouse-planning/internal/adapters/outbound/postgres"
	"github.com/claudioed/warehouse-planning/internal/adapters/outbound/postgres/pgtx"
	"github.com/claudioed/warehouse-planning/internal/application/outbox"
	"github.com/claudioed/warehouse-planning/internal/domain/capacityplan"
	"github.com/claudioed/warehouse-planning/internal/domain/processcapacity"
)

// pgtxTemplateDB is the once-migrated template every test clones.
const pgtxTemplateDB = "pgtx_it_migrated_template"

var (
	pgtxBaseURL string
	pgtxDBSeq   atomic.Uint64
)

func TestMain(m *testing.M) {
	os.Exit(pgtxRunTests(m))
}

func pgtxRunTests(m *testing.M) int {
	ctx := context.Background()
	container, err := tcpostgres.Run(ctx, "postgres:16-alpine",
		tcpostgres.WithDatabase("wp_pgtx_it"),
		tcpostgres.WithUsername("wp"),
		tcpostgres.WithPassword("wp"),
		testcontainers.WithWaitStrategy(
			wait.ForLog("database system is ready to accept connections").
				WithOccurrence(2).
				WithStartupTimeout(90*time.Second),
		),
	)
	if err != nil {
		fmt.Fprintf(os.Stderr, "start postgres container: %v\n", err)
		return 1
	}
	defer func() {
		if err := testcontainers.TerminateContainer(container); err != nil {
			fmt.Fprintf(os.Stderr, "terminate postgres container: %v\n", err)
		}
	}()

	if pgtxBaseURL, err = container.ConnectionString(ctx, "sslmode=disable"); err != nil {
		fmt.Fprintf(os.Stderr, "postgres connection string: %v\n", err)
		return 1
	}

	if err := pgtxCreateDatabase(ctx, pgtxTemplateDB); err != nil {
		fmt.Fprintf(os.Stderr, "create template database: %v\n", err)
		return 1
	}
	_, thisFile, _, _ := runtime.Caller(0)
	migrations := filepath.Join(filepath.Dir(thisFile), "..", "migrations")
	if err := postgres.RunMigrations(pgtxWithDB(pgtxBaseURL, pgtxTemplateDB), migrations); err != nil {
		fmt.Fprintf(os.Stderr, "migrate template: %v\n", err)
		return 1
	}
	return m.Run()
}

// pgtxWithDB rewrites the path of a connection URL to the named database.
func pgtxWithDB(baseURL, name string) string {
	u, err := url.Parse(baseURL)
	if err != nil {
		panic(err)
	}
	u.Path = "/" + name
	return u.String()
}

// pgtxCreateDatabase creates an empty database inside the shared container.
func pgtxCreateDatabase(ctx context.Context, name string) error {
	conn, err := pgx.Connect(ctx, pgtxBaseURL)
	if err != nil {
		return fmt.Errorf("connect: %w", err)
	}
	defer conn.Close(ctx)
	if _, err := conn.Exec(ctx, fmt.Sprintf("CREATE DATABASE %q", name)); err != nil {
		return fmt.Errorf("create database %s: %w", name, err)
	}
	return nil
}

// pgtxMigratedPool hands the test a pool over its own private database,
// cloned from the migrated template.
func pgtxMigratedPool(t *testing.T) *pgxpool.Pool {
	t.Helper()
	name := fmt.Sprintf("pgtx_it_%d", pgtxDBSeq.Add(1))
	conn, err := pgx.Connect(context.Background(), pgtxBaseURL)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer conn.Close(context.Background())
	if _, err := conn.Exec(context.Background(), fmt.Sprintf(
		"CREATE DATABASE %q WITH TEMPLATE %q", name, pgtxTemplateDB)); err != nil {
		t.Fatalf("clone database: %v", err)
	}
	pool, err := postgres.NewPool(context.Background(), pgtxWithDB(pgtxBaseURL, name))
	if err != nil {
		t.Fatalf("open pool: %v", err)
	}
	t.Cleanup(pool.Close)
	return pool
}

// itPlan builds a minimal valid DRAFT plan directly through the aggregate
// (no path-capacity fixture needed: the aggregate performs no I/O).
func itPlan(t *testing.T, id string) *capacityplan.CapacityPlan {
	t.Helper()
	window, err := processcapacity.NewCapacityWindow(
		time.Date(2026, 10, 5, 8, 0, 0, 0, time.UTC),
		time.Date(2026, 10, 5, 16, 0, 0, 0, time.UTC))
	if err != nil {
		t.Fatalf("window: %v", err)
	}
	rate, err := processcapacity.NewCapacityRate(1000, processcapacity.UnitOrder, time.Hour)
	if err != nil {
		t.Fatalf("rate: %v", err)
	}
	plan, err := capacityplan.Create(capacityplan.CreateParams{
		ID: id, WarehouseID: "WH-1", SiteID: "SIM1", Location: "PGTX-LOC",
		Window: window, ProcessPathID: "pick-rebin-pack", AssignedDemand: 12000,
		PathRate: rate, BottleneckStep: "REBIN",
	}, time.Date(2026, 10, 4, 17, 15, 30, 0, time.UTC))
	if err != nil {
		t.Fatalf("create plan: %v", err)
	}
	return plan
}

// pgtxCounts returns the (capacity_plans, outbox_events) row counts.
func pgtxCounts(t *testing.T, pool *pgxpool.Pool) (int, int) {
	t.Helper()
	var plans, events int
	if err := pool.QueryRow(context.Background(), `SELECT count(*) FROM capacity_plans`).Scan(&plans); err != nil {
		t.Fatal(err)
	}
	if err := pool.QueryRow(context.Background(), `SELECT count(*) FROM outbox_events`).Scan(&events); err != nil {
		t.Fatal(err)
	}
	return plans, events
}

// itEvents returns the plan's domain events for the outbox insert.
func itEvents(plan *capacityplan.CapacityPlan) []capacityplan.Event {
	return plan.PullEvents()
}

// THE all-or-nothing contract on the repo's real write flow: inside ONE
// postgres.UnitOfWork (whose transaction travels via pgtx), a plan Save
// plus a real outbox Insert commit together — and the data is visible to a
// DIFFERENT connection afterwards, proving a real COMMIT happened.
func TestPgtx_UnitOfWorkCommitsPlanAndOutboxAtomically(t *testing.T) {
	pool := pgtxMigratedPool(t)
	plans, ob := postgres.NewCapacityPlanRepo(pool), postgres.NewOutboxRepo(pool)
	uow := postgres.NewUnitOfWork(pool)
	ctx := context.Background()

	plan := itPlan(t, "plan-pgtx-commit")
	err := uow.Do(ctx, func(txCtx context.Context) error {
		// The scope's ctx really carries the transaction the repos join.
		if _, ok := pgtx.From(txCtx); !ok {
			t.Error("fn's ctx carries no transaction")
		}
		if err := plans.Save(txCtx, plan); err != nil {
			return err
		}
		// A real second write in the SAME bracket: one raw outbox row
		// shaped like the use cases' enqueue would insert it.
		return ob.Insert(txCtx, itOutboxMessage("pgtx-e1", plan.ID()))
	})
	if err != nil {
		t.Fatalf("Do: %v", err)
	}
	if p, o := pgtxCounts(t, pool); p != 1 || o != 1 {
		t.Fatalf("after commit: plans=%d outbox=%d, want 1/1", p, o)
	}
	// The plan persisted with its full state.
	got, err := plans.FindByID(ctx, "plan-pgtx-commit")
	if err != nil || got == nil {
		t.Fatalf("FindByID = %v, %v", got, err)
	}
	if got.Status() != capacityplan.StatusDraft || got.Shortage() != 4000 {
		t.Fatalf("persisted plan = %s shortage %v", got.Status(), got.Shortage())
	}
}

// The rollback half: an error inside the UnitOfWork scope — raised AFTER
// both real writes succeeded — must roll EVERYTHING back. Neither the plan
// row nor the outbox row survives.
func TestPgtx_ErrorInsideScopeRollsBackPlanAndOutbox(t *testing.T) {
	pool := pgtxMigratedPool(t)
	plans, ob := postgres.NewCapacityPlanRepo(pool), postgres.NewOutboxRepo(pool)
	uow := postgres.NewUnitOfWork(pool)
	ctx := context.Background()

	boom := errors.New("boom after both writes")
	plan := itPlan(t, "plan-pgtx-rollback")
	err := uow.Do(ctx, func(txCtx context.Context) error {
		if err := plans.Save(txCtx, plan); err != nil {
			return err
		}
		if err := ob.Insert(txCtx, itOutboxMessage("pgtx-e2", plan.ID())); err != nil {
			return err
		}
		return boom // both writes already succeeded
	})
	if !errors.Is(err, boom) {
		t.Fatalf("Do err = %v, want boom", err)
	}
	if p, o := pgtxCounts(t, pool); p != 0 || o != 0 {
		t.Fatalf("after rollback: plans=%d outbox=%d, want 0/0 — the bracket is not all-or-nothing", p, o)
	}
}

// A failing STATEMENT inside the scope (a real Postgres error, not an
// injected wrapper) also rolls back the earlier writes — the transaction
// pgtx carries is the real serialization point.
func TestPgtx_RealStatementErrorRollsBackEarlierWrites(t *testing.T) {
	pool := pgtxMigratedPool(t)
	plans := postgres.NewCapacityPlanRepo(pool)
	uow := postgres.NewUnitOfWork(pool)
	ctx := context.Background()

	plan := itPlan(t, "plan-pgtx-stmt")
	err := uow.Do(ctx, func(txCtx context.Context) error {
		if err := plans.Save(txCtx, plan); err != nil {
			return err
		}
		tx, _ := pgtx.From(txCtx)
		_, err := tx.Exec(txCtx, "INSERT INTO table_that_does_not_exist VALUES (1)")
		return err
	})
	if err == nil {
		t.Fatal("expected the undefined-table error")
	}
	if p, o := pgtxCounts(t, pool); p != 0 || o != 0 {
		t.Fatalf("after statement failure: plans=%d outbox=%d, want 0/0", p, o)
	}
}

// The context mechanics themselves, on a real transaction handle.
func TestPgtx_ContextCarriesTheTransaction(t *testing.T) {
	pool := pgtxMigratedPool(t)
	ctx := context.Background()

	// From on a plain ctx: no transaction, ok=false.
	if _, ok := pgtx.From(ctx); ok {
		t.Fatal("plain ctx must not carry a transaction")
	}

	tx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	defer func() { _ = tx.Rollback(context.WithoutCancel(ctx)) }()

	txCtx := pgtx.With(ctx, tx)
	got, ok := pgtx.From(txCtx)
	if !ok || got != tx {
		t.Fatalf("From(With(ctx, tx)) = %v, %v; want the same tx", got, ok)
	}

	// A write through the carried tx is visible inside the scope but NOT
	// outside it (before commit) — the ctx really routes to the tx.
	if _, err := got.Exec(txCtx, `INSERT INTO outbox_events (event_id, topic, event_type, subject, dataschema, value, headers) VALUES ('pgtx-ctx-1', 't', 'T', 's', 'u', '{}'::bytea, '[]'::jsonb)`); err != nil {
		t.Fatalf("exec through carried tx: %v", err)
	}
	var inside int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM outbox_events WHERE event_id = 'pgtx-ctx-1'`).Scan(&inside); err != nil {
		t.Fatal(err)
	}
	if inside != 0 {
		t.Fatal("the uncommitted write leaked outside the transaction")
	}
}

// itOutboxMessage builds a minimal outbox row in the repository's insert
// shape.
func itOutboxMessage(eventID, subject string) outbox.Message {
	return outbox.Message{
		EventID: eventID, Topic: "warehouse.warehouse-planning.events",
		EventType: "com.warehouse.wes.warehouse-planning.capacityplan.CapacityPlanCreated",
		Subject:   subject, DataSchema: "urn:warehouse:warehouse-planning:events:CapacityPlanCreated:v1",
		Value:   []byte(`{}`),
		Headers: []outbox.Header{{Key: "content-type", Value: "application/cloudevents+json; charset=UTF-8"}},
	}
}
