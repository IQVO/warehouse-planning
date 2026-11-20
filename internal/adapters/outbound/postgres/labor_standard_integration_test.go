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
	"github.com/claudioed/warehouse-planning/internal/application/usecases"
	"github.com/claudioed/warehouse-planning/internal/domain/laborstandard"
)

var (
	lsFrom   = time.Date(2026, 11, 2, 6, 0, 0, 0, time.UTC)
	lsWindow = time.Date(2026, 11, 20, 8, 0, 0, 0, time.UTC)
)

func lsStandard(t *testing.T, id string, mutate func(*laborstandard.Params)) laborstandard.Standard {
	t.Helper()
	p := laborstandard.Params{StandardID: id, TaskType: "PICK", ExpectedSeconds: 45, EffectiveFrom: lsFrom, AsOf: lsFrom}
	if mutate != nil {
		mutate(&p)
	}
	s, err := laborstandard.New(p)
	if err != nil {
		t.Fatal(err)
	}
	return s
}

// The Postgres copy and the in-memory one hold the same facts and answer the
// same InForce for every (site, task type, window start): the SQL only narrows
// the candidates, laborstandard.Resolve decides (docs/adr/0022 decision 3).
func TestLaborStandardRepo_RecordsFactsAndResolvesLikeTheInMemoryAdapter(t *testing.T) {
	pool := newMigratedPool(t)
	pg, mem := postgres.NewLaborStandardRepo(pool), memory.NewLaborStandardRepo()
	ctx := context.Background()
	travel := int64(15)
	standards := []laborstandard.Standard{
		lsStandard(t, "std-1", func(p *laborstandard.Params) { p.TravelComponentSeconds = &travel }),
		lsStandard(t, "std-2", func(p *laborstandard.Params) {
			p.EffectiveFrom, p.AsOf, p.ExpectedSeconds = lsFrom.Add(24*time.Hour), lsFrom.Add(24*time.Hour), 40
		}),
		lsStandard(t, "std-3", func(p *laborstandard.Params) { p.SiteCode, p.ExpectedSeconds = "SIM1", 50 }),
		lsStandard(t, "std-4", func(p *laborstandard.Params) { p.SiteCode, p.EffectiveFrom = "SIM1", lsWindow.Add(time.Second) }),
		lsStandard(t, "std-5", func(p *laborstandard.Params) { p.HandlingUnit, p.SiteCode = laborstandard.HandlingUnitCase, "SIM1" }),
		lsStandard(t, "std-6", func(p *laborstandard.Params) { p.TaskType, p.ExpectedSeconds = "PACK", 30 }),
		lsStandard(t, "std-7", func(p *laborstandard.Params) { p.SiteCode, p.EffectiveFrom = "SIM2", lsWindow }),
	}
	for _, s := range standards {
		for name, repo := range map[string]interface {
			Record(context.Context, laborstandard.Standard) (bool, error)
		}{"postgres": pg, "memory": mem} {
			if recorded, err := repo.Record(ctx, s); err != nil || !recorded {
				t.Fatalf("%s Record(%s) = %v, %v", name, s.ID(), recorded, err)
			}
		}
	}
	// Same id again: a no-op, the first record is kept.
	again := lsStandard(t, "std-1", func(p *laborstandard.Params) { p.ExpectedSeconds = 99 })
	if recorded, err := pg.Record(ctx, again); err != nil || recorded {
		t.Fatalf("Record of an id already stored = %v, %v; want false, nil", recorded, err)
	}

	assertInForceLikeMemory(ctx, t, pg, mem)
	got, _, err := pg.InForce(ctx, "SIM3", "PICK", lsFrom.Add(time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	assertStd1(t, got)
}

// assertInForceLikeMemory asks both copies the same InForce questions.
func assertInForceLikeMemory(ctx context.Context, t *testing.T, pg *postgres.LaborStandardRepo, mem *memory.LaborStandardRepo) {
	t.Helper()
	for _, q := range []struct {
		site, taskType string
		at             time.Time
		wantID         string
	}{
		{"SIM1", "PICK", lsWindow, "std-3"},
		{"SIM2", "PICK", lsWindow, "std-7"},
		{"SIM2", "PICK", lsWindow.Add(-time.Second), "std-2"},
		{"SIM3", "PICK", lsWindow, "std-2"},
		{"SIM3", "PICK", lsFrom.Add(time.Hour), "std-1"},
		{"SIM3", "PICK", lsFrom.Add(-time.Second), ""},
		{"SIM1", "PACK", lsWindow, "std-6"},
		{"SIM1", "SLAM", lsWindow, ""},
	} {
		got, ok, err := pg.InForce(ctx, q.site, q.taskType, q.at)
		if err != nil {
			t.Fatal(err)
		}
		want, wantOK, _ := mem.InForce(ctx, q.site, q.taskType, q.at)
		if ok != wantOK || got.ID() != want.ID() || got.ID() != q.wantID {
			t.Fatalf("InForce(%s, %s, %s) = %q (%v), memory %q (%v), want %q", q.site, q.taskType, q.at, got.ID(), ok, want.ID(), wantOK, q.wantID)
		}
	}
}

// assertStd1 checks every field of std-1 read back from Postgres.
func assertStd1(t *testing.T, got laborstandard.Standard) {
	t.Helper()
	if got.ExpectedSeconds() != 45 || got.TravelComponentSeconds() == nil || *got.TravelComponentSeconds() != 15 ||
		!got.EffectiveFrom().Equal(lsFrom) || !got.AsOf().Equal(lsFrom) || got.SiteCode() != "" {
		t.Fatalf("std-1 read back as %+v", got)
	}
}

// The claim (consumer labor-standard-consumer) and the record commit or roll
// back together, through the real use case.
func TestLaborStandardRepo_ClaimAndRecordCommitOrRollBackTogether(t *testing.T) {
	pool := newMigratedPool(t)
	uow := postgres.NewUnitOfWork(pool)
	repo := postgres.NewLaborStandardRepo(pool)
	processed := postgres.NewProcessedEventRepo(pool)
	ctx := context.Background()
	s := lsStandard(t, "std-1", nil)
	boom := errors.New("injected failure after the write")

	err := uow.Do(ctx, func(ctx context.Context) error {
		if claimed, err := processed.Claim(ctx, "labor-standard-consumer", "evt-1"); err != nil || !claimed {
			return fmt.Errorf("claim = %v: %w", claimed, err)
		}
		if _, err := repo.Record(ctx, s); err != nil {
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
	if count("SELECT count(*) FROM labor_standards") != 0 || count("SELECT count(*) FROM processed_events WHERE consumer='labor-standard-consumer'") != 0 {
		t.Fatal("a failed unit of work left a standard or a claim behind")
	}
	record := &usecases.RecordLaborStandard{UoW: uow, ProcessedEvents: processed, Standards: repo}
	for i, want := range []usecases.RecordOutcome{usecases.OutcomeApplied, usecases.OutcomeDuplicate} {
		outcome, err := record.Handle(ctx, "labor-standard-consumer", "evt-1", s)
		if err != nil || outcome != want {
			t.Fatalf("delivery %d = %s, %v; want %s", i+1, outcome, err, want)
		}
	}
	if outcome, err := record.Handle(ctx, "labor-standard-consumer", "evt-2", s); err != nil || outcome != usecases.OutcomeStale {
		t.Fatalf("same standard id under a new event id = %s, %v; want stale", outcome, err)
	}
	if count("SELECT count(*) FROM labor_standards") != 1 || count("SELECT count(*) FROM processed_events WHERE consumer='labor-standard-consumer'") != 2 {
		t.Fatal("the copy must hold one row per standard id and one claim per event id")
	}
}

// The table refuses what the domain refuses, and migration 0013 is
// reversible.
func TestMigration0013_ChecksAndDownUp(t *testing.T) {
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
		"zero expected_seconds":  `INSERT INTO labor_standards (standard_id, task_type, expected_seconds, effective_from, as_of) VALUES ('x', 'PICK', 0, now(), now())`,
		"negative travel":        `INSERT INTO labor_standards (standard_id, task_type, expected_seconds, travel_component_seconds, effective_from, as_of) VALUES ('x', 'PICK', 4, -1, now(), now())`,
		"unknown handling unit":  `INSERT INTO labor_standards (standard_id, task_type, handling_unit, expected_seconds, effective_from, as_of) VALUES ('x', 'PICK', 'INNER', 4, now(), now())`,
		"blank task type":        `INSERT INTO labor_standards (standard_id, task_type, expected_seconds, effective_from, as_of) VALUES ('x', '', 4, now(), now())`,
		"missing effective_from": `INSERT INTO labor_standards (standard_id, task_type, expected_seconds, as_of) VALUES ('x', 'PICK', 4, now())`,
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
	if err := m.Migrate(12); err != nil {
		t.Fatalf("down 0013: %v", err)
	}
	var tables int
	if err := pool.QueryRow(ctx, "SELECT count(*) FROM information_schema.tables WHERE table_name = 'labor_standards'").Scan(&tables); err != nil || tables != 0 {
		t.Fatalf("labor_standards survived the down migration: %d, %v", tables, err)
	}
	var columns int
	if err := pool.QueryRow(ctx, "SELECT count(*) FROM information_schema.columns WHERE table_name = 'capacity_plans' AND column_name = 'labour_requirement'").Scan(&columns); err != nil || columns != 0 {
		t.Fatalf("capacity_plans.labour_requirement survived the down migration: %d, %v", columns, err)
	}
	if err := m.Up(); err != nil {
		t.Fatalf("up again: %v", err)
	}
}
