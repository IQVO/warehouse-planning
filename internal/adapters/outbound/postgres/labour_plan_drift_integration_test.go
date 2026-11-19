//go:build integration

package postgres_test

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/golang-migrate/migrate/v4"
	"github.com/jackc/pgx/v5/pgxpool"

	outboundkafka "github.com/claudioed/warehouse-planning/internal/adapters/outbound/kafka"
	"github.com/claudioed/warehouse-planning/internal/adapters/outbound/postgres"
	"github.com/claudioed/warehouse-planning/internal/application/usecases"
	"github.com/claudioed/warehouse-planning/internal/domain/labourplan"
)

var errDriftRecord = errors.New("drift record failed")

// driftRecordFails fails every RecordDrift after the upsert of the commitment.
type driftRecordFails struct{ *postgres.CommittedHeadsRepo }

func (driftRecordFails) RecordDrift(context.Context, string, string, string, int) error {
	return errDriftRecord
}

// driftStack is both labour plan drift triggers (docs/adr/0023 decision 3)
// on Postgres: the commitment trigger inside a consumer-shaped unit of work
// (processed-event claim first, exactly as LaborCapacityConsumer runs it) and
// the plan trigger, on the real fan-out encoder and outbox.
type driftStack struct {
	pool      *pgxpool.Pool
	uow       *postgres.UnitOfWork
	processed *postgres.ProcessedEventRepo
	record    *usecases.RecordShiftCommitment
	commit    *usecases.CommitLabourPlan
}

func newDriftStack(t *testing.T) *driftStack {
	t.Helper()
	pool := newMigratedPool(t)
	uow, plans, commitments, ob := postgres.NewUnitOfWork(pool), postgres.NewLabourPlanRepo(pool), postgres.NewCommittedHeadsRepo(pool), postgres.NewOutboxRepo(pool)
	enc := outboundkafka.NewFanoutEncoder()
	return &driftStack{
		pool: pool, uow: uow, processed: postgres.NewProcessedEventRepo(pool),
		record: &usecases.RecordShiftCommitment{Commitments: commitments, Plans: plans, Outbox: ob, Encoder: enc, UnitOfWork: uow},
		commit: &usecases.CommitLabourPlan{Plans: plans, Outbox: ob, Encoder: enc, UnitOfWork: uow, Commitments: commitments},
	}
}

// deliver is one ShiftPlanCommitted line of SIM1/S1/PICK committing heads,
// claimed under eventID in the same transaction (a claimed id does nothing).
func (s *driftStack) deliver(t *testing.T, eventID string, heads int) error {
	t.Helper()
	return s.uow.Do(context.Background(), func(ctx context.Context) error {
		claimed, err := s.processed.Claim(ctx, "labor-capacity-consumer", eventID)
		if err != nil || !claimed {
			return err
		}
		return s.record.Handle(ctx, usecases.RecordShiftCommitmentCommand{
			SiteID: "SIM1", ShiftID: "S1", PathID: "PICK", CommittedHeads: heads,
			ObservedAt: time.Date(2026, 11, 19, 6, 0, 0, 0, time.UTC),
		})
	})
}

func (s *driftStack) plan(t *testing.T, heads int) {
	t.Helper()
	if _, err := s.commit.Handle(context.Background(), usecases.CommitLabourPlanCommand{
		SiteID: "SIM1", ShiftID: "S1", PathID: "PICK", PlannedHeads: &heads, RateUnitsPerHour: 10, Hours: 8, InstalledStations: 12,
	}); err != nil {
		t.Fatal(err)
	}
}

// drifts returns the drift_heads of every LabourPlanDriftDetected outbox row
// on topic, in insertion order.
func (s *driftStack) drifts(t *testing.T, topic string) []int {
	t.Helper()
	var out []int
	for _, r := range outboxRows(t, s.pool) {
		if r.topic != topic || !strings.HasSuffix(r.eventType, "."+labourplan.EventLabourPlanDriftDetected) {
			continue
		}
		var env struct {
			Data struct {
				DriftHeads int `json:"drift_heads"`
			} `json:"data"`
		}
		if err := json.Unmarshal([]byte(r.value), &env); err != nil {
			t.Fatal(err)
		}
		out = append(out, env.Data.DriftHeads)
	}
	return out
}

// row reads the SIM1/S1/PICK commitment row; last is nil when no drift is recorded.
func (s *driftStack) row(t *testing.T) (heads int, last *int, found bool) {
	t.Helper()
	err := s.pool.QueryRow(context.Background(), `SELECT committed_heads, last_drift_heads FROM labour_plan_commitments
		WHERE site_id = 'SIM1' AND shift_id = 'S1' AND path_id = 'PICK'`).Scan(&heads, &last)
	if err != nil {
		return 0, nil, false
	}
	return heads, last, true
}

func wantDriftsOnBothTopics(t *testing.T, s *driftStack, want string) {
	t.Helper()
	for _, topic := range []string{integrationTopic, analyticsTopic} {
		got, _ := json.Marshal(s.drifts(t, topic))
		if string(got) != want {
			t.Fatalf("%s drifts = %s, want %s", topic, got, want)
		}
	}
}

// Both triggers on Postgres: a commitment before the plan stores the heads
// and raises nothing; the plan raises the drift; the same drift again, a
// redelivery and an unchanged PUT raise nothing; a drift that clears (zero
// recorded) and returns is raised again; every raise lands on both topics.
func TestLabourPlanDrift_BothTriggersOnPostgres(t *testing.T) {
	s := newDriftStack(t)
	if err := s.deliver(t, "wfm-1", 10); err != nil {
		t.Fatal(err)
	}
	if heads, last, found := s.row(t); !found || heads != 10 || last != nil {
		t.Fatalf("row = %d, %v, %v; want 10 heads and no drift", heads, last, found)
	}
	wantDriftsOnBothTopics(t, s, "null")

	s.plan(t, 7)
	wantDriftsOnBothTopics(t, s, "[3]")
	s.plan(t, 7)
	for _, step := range []struct {
		id    string
		heads int
	}{{"wfm-2", 10}, {"wfm-1", 7}, {"wfm-3", 7}, {"wfm-4", 10}} {
		if err := s.deliver(t, step.id, step.heads); err != nil {
			t.Fatal(err)
		}
	}
	wantDriftsOnBothTopics(t, s, "[3,3]")
	if heads, last, _ := s.row(t); heads != 10 || last == nil || *last != 3 {
		t.Fatalf("row = %d, %v; want 10 heads with drift 3 recorded", heads, last)
	}
}

// A failure after the commitment upsert rolls back the claim, the commitment
// and the outbox rows of the same transaction.
func TestLabourPlanDrift_AFailedDriftRecordRollsTheMessageBackOnPostgres(t *testing.T) {
	s := newDriftStack(t)
	s.plan(t, 7)
	s.record.Commitments = driftRecordFails{postgres.NewCommittedHeadsRepo(s.pool)}
	if err := s.deliver(t, "wfm-1", 10); !errors.Is(err, errDriftRecord) {
		t.Fatalf("err = %v, want the drift record failure", err)
	}
	if _, _, found := s.row(t); found {
		t.Fatal("the commitment survived the rolled-back transaction")
	}
	wantDriftsOnBothTopics(t, s, "null")
	claimed, err := s.processed.Claim(context.Background(), "labor-capacity-consumer", "wfm-1")
	if err != nil || !claimed {
		t.Fatalf("claim after the rollback = %v, %v; the retry would be skipped", claimed, err)
	}
}

// The commitments table refuses what the drift refuses (a blank key, negative
// heads, no observation time), and migration 0015 is reversible: down to
// 0014 drops only labour_plan_commitments, up again recreates it.
func TestMigration0015_ChecksAndUpDownUp(t *testing.T) {
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
		"blank site":      `INSERT INTO labour_plan_commitments VALUES ('', 'day', 'p', 1, now(), NULL)`,
		"blank shift":     `INSERT INTO labour_plan_commitments VALUES ('SIM1', '', 'p', 1, now(), NULL)`,
		"blank path":      `INSERT INTO labour_plan_commitments VALUES ('SIM1', 'day', '', 1, now(), NULL)`,
		"negative heads":  `INSERT INTO labour_plan_commitments VALUES ('SIM1', 'day', 'p', -1, now(), NULL)`,
		"heads overflow":  `INSERT INTO labour_plan_commitments VALUES ('SIM1', 'day', 'p', 2147483648, now(), NULL)`,
		"no observed_at":  `INSERT INTO labour_plan_commitments (site_id, shift_id, path_id, committed_heads) VALUES ('SIM1', 'day', 'p', 1)`,
		"no heads at all": `INSERT INTO labour_plan_commitments (site_id, shift_id, path_id, observed_at) VALUES ('SIM1', 'day', 'p', now())`,
	} {
		if _, err := pool.Exec(ctx, q); err == nil {
			t.Errorf("%s: the table accepted a row the drift refuses", name)
		}
	}
	if _, err := pool.Exec(ctx, `INSERT INTO labour_plan_commitments VALUES ('SIM1', 'day', 'p', 0, now(), -4)`); err != nil {
		t.Fatalf("a zero commitment with a negative recorded drift is valid: %v", err)
	}

	m, err := migrate.New("file://"+migrationsDir(t), databaseURL)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _, _ = m.Close() }()
	if err := m.Migrate(14); err != nil {
		t.Fatalf("down 0015: %v", err)
	}
	count := func(table string) int {
		var n int
		if err := pool.QueryRow(ctx, `SELECT count(*) FROM information_schema.tables WHERE table_name = $1`, table).Scan(&n); err != nil {
			t.Fatal(err)
		}
		return n
	}
	if count("labour_plan_commitments") != 0 {
		t.Fatal("labour_plan_commitments survived the down migration")
	}
	if count("labour_plans") != 1 {
		t.Fatal("down 0015 dropped labour_plans: the migration is not additive")
	}
	if err := m.Up(); err != nil {
		t.Fatalf("up again: %v", err)
	}
	if count("labour_plan_commitments") != 1 {
		t.Fatal("up again did not recreate labour_plan_commitments")
	}
}
