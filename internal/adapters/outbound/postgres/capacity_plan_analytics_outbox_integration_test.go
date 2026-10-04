//go:build integration

package postgres_test

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"

	outboundkafka "github.com/claudioed/warehouse-planning/internal/adapters/outbound/kafka"
	"github.com/claudioed/warehouse-planning/internal/application/outbox"
)

// ADR 0005: every domain event is enqueued TWICE in the same unit of work, on
// the integration topic and on the analytics topic, under ONE CloudEvents id.

const (
	integrationTopic = "warehouse.warehouse-planning.events"
	analyticsTopic   = "warehouse.warehouse-planning.analytics"
)

func newFanoutPlanStack(t *testing.T) *planStack {
	t.Helper()
	s := newPlanStack(t)
	enc := outboundkafka.NewFanoutEncoder()
	s.create.Encoder, s.publish.Encoder = enc, enc
	return s
}

type outboxRow struct {
	eventID, topic, eventType, dataschema, value string
}

func outboxRows(t *testing.T, pool *pgxpool.Pool) []outboxRow {
	t.Helper()
	rows, err := pool.Query(context.Background(), `SELECT event_id, topic, event_type, dataschema, convert_from(value, 'UTF8') FROM outbox_events ORDER BY id`)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	var out []outboxRow
	for rows.Next() {
		var r outboxRow
		if err := rows.Scan(&r.eventID, &r.topic, &r.eventType, &r.dataschema, &r.value); err != nil {
			t.Fatal(err)
		}
		out = append(out, r)
	}
	return out
}

func TestFanout_Postgres_EachEventLandsOnBothTopicsUnderOneIDInOneTransaction(t *testing.T) {
	s := newFanoutPlanStack(t)
	ctx := context.Background()
	plan, err := s.create.Handle(ctx, planCmd(12000))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.publish.Handle(ctx, plan.ID()); err != nil {
		t.Fatal(err)
	}

	rows := outboxRows(t, s.pool)
	if len(rows) != 8 {
		t.Fatalf("outbox rows = %d, want 8 (four events x two topics)", len(rows))
	}
	for i, ev := range []string{"CapacityPlanCreated", "CapacityPlanPublished", "CapacityShortageDetected", "BottleneckDetected"} {
		integration, analytics := rows[2*i], rows[2*i+1]
		if integration.topic != integrationTopic || analytics.topic != analyticsTopic {
			t.Fatalf("event %d topics = %q, %q", i, integration.topic, analytics.topic)
		}
		if integration.eventID != analytics.eventID || integration.eventID == "" {
			t.Errorf("%s: ids %q vs %q must be the SAME per occurrence", ev, integration.eventID, analytics.eventID)
		}
		if integration.eventType != typePrefix+ev || analytics.eventType != integration.eventType {
			t.Errorf("%s: types %q / %q", ev, integration.eventType, analytics.eventType)
		}
		if integration.dataschema != "urn:warehouse:warehouse-planning:events:"+ev+":v1" || analytics.dataschema != "urn:warehouse:warehouse-planning:analytics:"+ev+":v1" {
			t.Errorf("%s: dataschemas %q / %q", ev, integration.dataschema, analytics.dataschema)
		}
		if !strings.Contains(analytics.value, `"id":"`+analytics.eventID+`"`) || !strings.Contains(integration.value, `"id":"`+integration.eventID+`"`) {
			t.Errorf("%s: the persisted bytes do not carry the row's CloudEvents id", ev)
		}
	}
	// Ids are distinct across occurrences.
	seen := map[string]bool{}
	for i := 0; i < 8; i += 2 {
		if seen[rows[i].eventID] {
			t.Fatalf("occurrence id %q reused", rows[i].eventID)
		}
		seen[rows[i].eventID] = true
	}

	// binding_constraint rides on the analytics Published payload ONLY.
	published := rows[2:4]
	if strings.Contains(published[0].value, "binding_constraint") {
		t.Errorf("the integration payload leaked binding_constraint: %s", published[0].value)
	}
	if !strings.Contains(published[1].value, `"binding_constraint":"LABOR"`) {
		t.Errorf("the analytics payload lacks the binding constraint: %s", published[1].value)
	}
}

func TestFanout_Postgres_AFailureWritesNeitherTopicsRows(t *testing.T) {
	boom := errors.New("injected failure after the real write")
	ctx := context.Background()

	t.Run("create: outbox insert fails after both rows were written", func(t *testing.T) {
		s := newFanoutPlanStack(t)
		s.create.Outbox = outboxThenFail{s.outbox, boom}
		if _, err := s.create.Handle(ctx, planCmd(12000)); !errors.Is(err, boom) {
			t.Fatalf("err = %v", err)
		}
		if p, o := count(t, s.pool, `SELECT count(*) FROM capacity_plans`), count(t, s.pool, `SELECT count(*) FROM outbox_events`); p != 0 || o != 0 {
			t.Errorf("plans = %d, outbox = %d; want 0, 0 (neither the integration nor the analytics row survives)", p, o)
		}
	})

	t.Run("create: plan save fails", func(t *testing.T) {
		s := newFanoutPlanStack(t)
		s.create.Plans = plansThenFail{s.plans, boom}
		if _, err := s.create.Handle(ctx, planCmd(12000)); !errors.Is(err, boom) {
			t.Fatalf("err = %v", err)
		}
		if o := count(t, s.pool, `SELECT count(*) FROM outbox_events`); o != 0 {
			t.Errorf("outbox rows = %d, want 0", o)
		}
	})

	t.Run("publish: outbox insert fails -> only the Created pair remains", func(t *testing.T) {
		s := newFanoutPlanStack(t)
		plan, err := s.create.Handle(ctx, planCmd(12000))
		if err != nil {
			t.Fatal(err)
		}
		s.publish.Outbox = outboxThenFail{s.outbox, boom}
		if _, err := s.publish.Handle(ctx, plan.ID()); !errors.Is(err, boom) {
			t.Fatalf("err = %v", err)
		}
		if n := count(t, s.pool, `SELECT count(*) FROM outbox_events`); n != 2 {
			t.Errorf("outbox rows = %d, want 2 (Created on both topics only)", n)
		}
		if n := count(t, s.pool, `SELECT count(*) FROM outbox_events WHERE topic = $1`, analyticsTopic); n != 1 {
			t.Errorf("analytics rows = %d, want 1", n)
		}
	})
}

// Migration 0007: (event_id, topic) is the identity of an outbox row. The
// same id on another topic is fine; the same id twice on one topic is not.
func TestOutbox_Postgres_EventIDIsUniquePerTopic(t *testing.T) {
	s := newPlanStack(t)
	ctx := context.Background()
	msg := func(topic string) outbox.Message {
		return outbox.Message{EventID: "evt-1", Topic: topic, EventType: "T", Subject: "s", DataSchema: "d", Value: []byte("{}")}
	}
	if err := s.outbox.Insert(ctx, msg(integrationTopic), msg(analyticsTopic)); err != nil {
		t.Fatalf("same id on two topics: %v", err)
	}
	if err := s.outbox.Insert(ctx, msg(analyticsTopic)); err == nil {
		t.Fatal("the same (event_id, topic) was enqueued twice")
	}
	if n := count(t, s.pool, `SELECT count(*) FROM outbox_events`); n != 2 {
		t.Fatalf("rows = %d, want 2", n)
	}
}

func TestOutboxMigration0007_DownKeepsIntegrationRowsAndRestoresUniqueEventID(t *testing.T) {
	s := newFanoutPlanStack(t)
	ctx := context.Background()
	if _, err := s.create.Handle(ctx, planCmd(12000)); err != nil {
		t.Fatal(err)
	}
	down, err := os.ReadFile(filepath.Join(migrationsDir(t), "0007_outbox_event_id_per_topic.down.sql"))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.pool.Exec(ctx, string(down)); err != nil {
		t.Fatalf("down: %v", err)
	}
	if n := count(t, s.pool, `SELECT count(*) FROM outbox_events WHERE topic = $1`, integrationTopic); n != 1 {
		t.Errorf("integration rows after down = %d, want 1", n)
	}
	if n := count(t, s.pool, `SELECT count(*) FROM outbox_events WHERE topic = $1`, analyticsTopic); n != 0 {
		t.Errorf("analytics rows after down = %d, want 0", n)
	}
	var id string
	_ = s.pool.QueryRow(ctx, `SELECT event_id FROM outbox_events`).Scan(&id)
	if _, err := s.pool.Exec(ctx, `INSERT INTO outbox_events (event_id, topic, event_type, subject, dataschema, value) VALUES ($1, 'x', 't', 's', 'd', '\x7b7d')`, id); err == nil {
		t.Error("UNIQUE (event_id) was not restored by down")
	}
}
