//go:build integration

package outbox_test

import (
	"bytes"
	"context"
	"log/slog"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"

	relay "github.com/claudioed/warehouse-planning/internal/adapters/outbound/outbox"
	"github.com/claudioed/warehouse-planning/internal/adapters/outbound/postgres"
)

// insertRawOutboxRow writes one unpublished outbox_events row directly, so
// the attempt-cap tests control event_type and key exactly.
func insertRawOutboxRow(t *testing.T, pool *pgxpool.Pool, eventType, key string) int64 {
	t.Helper()
	var id int64
	err := pool.QueryRow(context.Background(), `
		INSERT INTO outbox_events (event_id, topic, event_type, subject, key, dataschema, value)
		VALUES (gen_random_uuid()::text, 'test.integration', $1, $2, $3, 'urn:test', '{"stub":true}')
		RETURNING id
	`, eventType, key, []byte(key)).Scan(&id)
	if err != nil {
		t.Fatalf("insert outbox row: %v", err)
	}
	return id
}

// outboxRowState returns attempts, last_error and whether the row is published.
func outboxRowState(t *testing.T, pool *pgxpool.Pool, id int64) (attempts int, lastErr string, published bool) {
	t.Helper()
	err := pool.QueryRow(context.Background(), `
		SELECT attempts, coalesce(last_error, ''), published_at IS NOT NULL
		FROM outbox_events WHERE id = $1
	`, id).Scan(&attempts, &lastErr, &published)
	if err != nil {
		t.Fatalf("read outbox row %d: %v", id, err)
	}
	return attempts, lastErr, published
}

func failingPasses(t *testing.T, r *relay.Relay, n int) {
	t.Helper()
	for i := 1; i <= n; i++ {
		published, err := r.RelayOnce(context.Background())
		if err == nil || published != 0 {
			t.Fatalf("pass %d: want the poison row to fail with nothing published, got published=%d err=%v", i, published, err)
		}
	}
}

// TestOutboxRelay_MaxAttempts_PoisonRowExcludedThenGoodRowPublishes is the
// gap-fix D12 reference test (ADR 0013) over real Postgres: a poison row
// blocks the relay only until it has failed maxAttempts times. The pass that
// records the last attempt logs it at ERROR with id, topic, type and key;
// from then on the row is excluded from claims (still in outbox_events with
// attempts and last_error) and the good row behind it, same key, publishes.
func TestOutboxRelay_MaxAttempts_PoisonRowExcludedThenGoodRowPublishes(t *testing.T) {
	pool := startPostgresPool(t)
	ctx := context.Background()
	poisonID := insertRawOutboxRow(t, pool, "Poison", "PLAN-CAP-1")
	goodID := insertRawOutboxRow(t, pool, "Good", "PLAN-CAP-1")

	var logs bytes.Buffer
	sink := &poisonSink{failOn: "Poison"}
	r := relay.NewRelay(postgres.NewOutboxRepo(pool), sink, slog.New(slog.NewJSONHandler(&logs, nil)), relay.WithMaxAttempts(3))

	failingPasses(t, r, 2)
	if recs := capRecords(t, &logs); len(recs) != 0 {
		t.Fatalf("no attempt-cap ERROR expected below the cap, got %v", recs)
	}
	failingPasses(t, r, 1)
	recs := capRecords(t, &logs)
	if len(recs) != 1 {
		t.Fatalf("want exactly 1 attempt-cap ERROR record, got %d", len(recs))
	}
	if got, _ := recs[0]["outbox_id"].(float64); int64(got) != poisonID {
		t.Errorf("outbox_id = %v, want %d", recs[0]["outbox_id"], poisonID)
	}
	for field, want := range map[string]string{"topic": "test.integration", "event_type": "Poison", "key": "PLAN-CAP-1"} {
		if recs[0][field] != want {
			t.Errorf("%s = %v, want %q", field, recs[0][field], want)
		}
	}

	if published, err := r.RelayOnce(ctx); err != nil || published != 1 {
		t.Fatalf("pass after the cap: want the good row published, got published=%d err=%v", published, err)
	}
	if len(sink.sent) != 1 || sink.sent[0] != "Good" {
		t.Fatalf("sink got %v, want only the Good row", sink.sent)
	}
	if _, _, ok := outboxRowState(t, pool, goodID); !ok {
		t.Fatal("the good row is not marked published")
	}
	if published, err := r.RelayOnce(ctx); err != nil || published != 0 {
		t.Fatalf("pass after drain: published=%d err=%v, want a no-op", published, err)
	}
	attempts, lastErr, ok := outboxRowState(t, pool, poisonID)
	if ok || attempts != 3 || lastErr != "message too large" {
		t.Fatalf("poison row: attempts=%d last_error=%q published=%v, want 3, the send error, unpublished", attempts, lastErr, ok)
	}
}

// TestOutboxRelay_MaxAttempts_ReArmedRowIsClaimedAgain proves the runbook's
// re-arm SQL: resetting attempts to 0 makes an excluded row claimable again.
func TestOutboxRelay_MaxAttempts_ReArmedRowIsClaimedAgain(t *testing.T) {
	pool := startPostgresPool(t)
	ctx := context.Background()
	poisonID := insertRawOutboxRow(t, pool, "Poison", "PLAN-CAP-2")

	sink := &poisonSink{failOn: "Poison"}
	r := relay.NewRelay(postgres.NewOutboxRepo(pool), sink, quiet(), relay.WithMaxAttempts(1))
	failingPasses(t, r, 1)
	if published, err := r.RelayOnce(ctx); err != nil || published != 0 {
		t.Fatalf("capped row was claimed again: published=%d err=%v", published, err)
	}

	if _, err := pool.Exec(ctx, `UPDATE outbox_events SET attempts = 0 WHERE id = $1`, poisonID); err != nil {
		t.Fatalf("re-arm: %v", err)
	}
	sink.failOn = ""
	if published, err := r.RelayOnce(ctx); err != nil || published != 1 {
		t.Fatalf("re-armed row: published=%d err=%v, want it published", published, err)
	}
}

// TestOutboxRelay_NoCap_PoisonRowBlocksLaterRows pins WithMaxAttempts(0)
// (OUTBOX_MAX_ATTEMPTS=0, unlimited): the poison row is retried forever and
// the row behind it never publishes. This is why every cmd/* relay must
// pass a cap (architecture fitness test TestOutboxRelaysPassAttemptCap).
func TestOutboxRelay_NoCap_PoisonRowBlocksLaterRows(t *testing.T) {
	pool := startPostgresPool(t)
	insertRawOutboxRow(t, pool, "Poison", "PLAN-CAP-3")
	goodID := insertRawOutboxRow(t, pool, "Good", "PLAN-CAP-3")

	sink := &poisonSink{failOn: "Poison"}
	r := relay.NewRelay(postgres.NewOutboxRepo(pool), sink, quiet(), relay.WithMaxAttempts(0))
	failingPasses(t, r, 5)
	if _, _, ok := outboxRowState(t, pool, goodID); ok || len(sink.sent) != 0 {
		t.Fatalf("with no cap the good row must stay blocked, published=%v sent=%d", ok, len(sink.sent))
	}
}
