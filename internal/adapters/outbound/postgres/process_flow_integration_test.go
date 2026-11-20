//go:build integration

package postgres_test

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/golang-migrate/migrate/v4"

	"github.com/claudioed/warehouse-planning/internal/adapters/outbound/postgres"
	"github.com/claudioed/warehouse-planning/internal/adapters/outbound/repocontract"
	"github.com/claudioed/warehouse-planning/internal/application/ports"
	"github.com/claudioed/warehouse-planning/internal/application/usecases"
	"github.com/claudioed/warehouse-planning/internal/domain/processflow"
)

var _ ports.ProcessFlowRepository = (*postgres.ProcessFlowRepo)(nil)

// The Postgres copy shows the behaviour the in-memory double shows
// (memory/process_flow_repository_test.go runs the same suite): the version
// guard lives in the UPSERT's WHERE, a retired flow does not resolve, and the
// active flows page by id. One database serves the suite; every case starts
// from an empty table.
func TestProcessFlowRepo_SatisfiesTheRepositoryContract(t *testing.T) {
	pool := newMigratedPool(t)
	repocontract.RunProcessFlows(t, func(t *testing.T) ports.ProcessFlowRepository {
		t.Helper()
		if _, err := pool.Exec(context.Background(), `TRUNCATE process_flows`); err != nil {
			t.Fatal(err)
		}
		return postgres.NewProcessFlowRepo(pool)
	})
}

// The claim (consumer process-flow-consumer) and the record commit or roll back
// together, through the real use case; a stale version is claimed but changes
// nothing.
func TestProcessFlowRepo_ClaimAndRecordCommitOrRollBackTogether(t *testing.T) {
	pool := newMigratedPool(t)
	uow := postgres.NewUnitOfWork(pool)
	repo := postgres.NewProcessFlowRepo(pool)
	processed := postgres.NewProcessedEventRepo(pool)
	ctx := context.Background()
	flow := func(version int64) processflow.Flow {
		f, err := processflow.New(processflow.Params{
			FlowID: "single-unit", Name: "Single unit", Version: version, Status: processflow.StatusActive,
			Stages:    []processflow.Stage{{PathID: "PICK", ProcessType: processflow.Pick}, {PathID: "PACK", ProcessType: processflow.Pack}},
			UpdatedAt: lsFrom,
		})
		if err != nil {
			t.Fatal(err)
		}
		return f
	}
	boom := errors.New("injected failure after the write")

	err := uow.Do(ctx, func(ctx context.Context) error {
		if claimed, err := processed.Claim(ctx, "process-flow-consumer", "evt-1"); err != nil || !claimed {
			return fmt.Errorf("claim = %v: %w", claimed, err)
		}
		if _, err := repo.Record(ctx, flow(1)); err != nil {
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
	if count("SELECT count(*) FROM process_flows") != 0 || count("SELECT count(*) FROM processed_events WHERE consumer='process-flow-consumer'") != 0 {
		t.Fatal("a failed unit of work left a flow or a claim behind")
	}
	record := &usecases.RecordProcessFlow{UoW: uow, ProcessedEvents: processed, Flows: repo}
	for i, step := range []struct {
		event   string
		version int64
		want    usecases.RecordOutcome
	}{
		{"evt-1", 1, usecases.OutcomeApplied},
		{"evt-1", 1, usecases.OutcomeDuplicate},
		{"evt-2", 1, usecases.OutcomeStale},
		{"evt-3", 2, usecases.OutcomeApplied},
	} {
		outcome, err := record.Handle(ctx, "process-flow-consumer", step.event, flow(step.version))
		if err != nil || outcome != step.want {
			t.Fatalf("delivery %d = %s, %v; want %s", i+1, outcome, err, step.want)
		}
	}
	if count("SELECT count(*) FROM process_flows WHERE version = 2") != 1 || count("SELECT count(*) FROM processed_events WHERE consumer='process-flow-consumer'") != 3 {
		t.Fatal("the copy must hold one row per flow id at the greatest version and one claim per event id")
	}
}

// The table refuses what the domain refuses, and migration 0018 is reversible.
func TestMigration0018_ChecksAndDownUp(t *testing.T) {
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
		"blank flow id":          `INSERT INTO process_flows (flow_id, name, stages, status, version, updated_at) VALUES ('', 'n', '[{"path_id":"A","process_type":"PICK"}]', 'ACTIVE', 1, now())`,
		"empty stages":           `INSERT INTO process_flows (flow_id, name, stages, status, version, updated_at) VALUES ('x', 'n', '[]', 'ACTIVE', 1, now())`,
		"stages is not an array": `INSERT INTO process_flows (flow_id, name, stages, status, version, updated_at) VALUES ('x', 'n', '{}', 'ACTIVE', 1, now())`,
		"unknown status":         `INSERT INTO process_flows (flow_id, name, stages, status, version, updated_at) VALUES ('x', 'n', '[{"path_id":"A","process_type":"PICK"}]', 'PAUSED', 1, now())`,
		"version zero":           `INSERT INTO process_flows (flow_id, name, stages, status, version, updated_at) VALUES ('x', 'n', '[{"path_id":"A","process_type":"PICK"}]', 'ACTIVE', 0, now())`,
		"missing updated_at":     `INSERT INTO process_flows (flow_id, name, stages, status, version) VALUES ('x', 'n', '[{"path_id":"A","process_type":"PICK"}]', 'ACTIVE', 1)`,
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
	if err := m.Migrate(17); err != nil {
		t.Fatalf("down 0018: %v", err)
	}
	var tables int
	if err := pool.QueryRow(ctx, "SELECT count(*) FROM information_schema.tables WHERE table_name = 'process_flows'").Scan(&tables); err != nil || tables != 0 {
		t.Fatalf("process_flows survived the down migration: %d, %v", tables, err)
	}
	if err := m.Up(); err != nil {
		t.Fatalf("up again: %v", err)
	}
}
