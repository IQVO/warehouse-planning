package main

import (
	"context"
	"io"
	"log/slog"
	"testing"
	"time"

	outboxrelay "github.com/claudioed/warehouse-planning/internal/adapters/outbound/outbox"
	"github.com/claudioed/warehouse-planning/internal/application/outbox"
)

func quietLogger() *slog.Logger { return slog.New(slog.NewTextHandler(io.Discard, nil)) }

func TestParsePublisherMode(t *testing.T) {
	for raw, want := range map[string]string{"": "log", "log": "log", "kafka": "kafka"} {
		got, err := parsePublisherMode(raw)
		if err != nil || got != want {
			t.Errorf("parsePublisherMode(%q) = %q, %v; want %q", raw, got, err, want)
		}
	}
	if _, err := parsePublisherMode("rabbit"); err == nil {
		t.Error("an unknown EVENT_PUBLISHER must be a config error")
	}
}

func TestParseRelayInterval(t *testing.T) {
	cases := map[string]time.Duration{
		"":      time.Second,            // unset -> default
		"250ms": 250 * time.Millisecond, // Go duration
		"3s":    3 * time.Second,        //
		"2":     2 * time.Second,        // plain seconds
		"0.5":   500 * time.Millisecond, //
		"abc":   time.Second,            // malformed -> default
		"-1s":   time.Second,            // non-positive -> default
		"0":     time.Second,            //
	}
	for raw, want := range cases {
		if got := parseRelayInterval(raw, quietLogger()); got != want {
			t.Errorf("parseRelayInterval(%q) = %v, want %v", raw, got, want)
		}
	}
}

type countingStore struct{ drains chan struct{} }

func (s countingStore) Drain(ctx context.Context, _ int, _ func(context.Context, outbox.Message) error) (int, error) {
	select {
	case s.drains <- struct{}{}:
	default:
	}
	return 0, ctx.Err()
}

var _ outboxrelay.Store = countingStore{}

func TestStartOutboxRelay_DefaultsToTheLogSinkAndStops(t *testing.T) {
	t.Setenv("EVENT_PUBLISHER", "")
	t.Setenv("OUTBOX_RELAY_INTERVAL", "10ms")
	store := countingStore{drains: make(chan struct{}, 1)}
	runner, stop, err := startOutboxRelay(store, quietLogger())
	if err != nil {
		t.Fatalf("startOutboxRelay: %v", err)
	}
	select {
	case <-store.drains:
	case <-time.After(5 * time.Second):
		t.Fatal("the relay never drained")
	}
	stop()
	select {
	case <-runner.done:
	case <-time.After(5 * time.Second):
		t.Fatal("the relay did not stop")
	}
}

// A broker that is down at boot must not crash or block startup: the Kafka
// writer is constructed but never dialled until the relay's first send.
func TestStartOutboxRelay_KafkaModeDoesNotDialAtBoot(t *testing.T) {
	t.Setenv("EVENT_PUBLISHER", "kafka")
	t.Setenv("KAFKA_BROKERS", "127.0.0.1:1") // nothing listens here
	t.Setenv("OUTBOX_RELAY_INTERVAL", "10ms")
	start := time.Now()
	runner, stop, err := startOutboxRelay(countingStore{drains: make(chan struct{}, 1)}, quietLogger())
	if err != nil {
		t.Fatalf("startOutboxRelay with an unreachable broker: %v", err)
	}
	if elapsed := time.Since(start); elapsed > time.Second {
		t.Errorf("startup took %v; it must not wait on Kafka", elapsed)
	}
	stop()
	select {
	case <-runner.done:
	case <-time.After(5 * time.Second):
		t.Fatal("the relay did not stop")
	}
}

func TestStartOutboxRelay_ConfigErrors(t *testing.T) {
	t.Run("kafka without brokers", func(t *testing.T) {
		t.Setenv("EVENT_PUBLISHER", "kafka")
		t.Setenv("KAFKA_BROKERS", "")
		if _, _, err := startOutboxRelay(countingStore{}, quietLogger()); err == nil {
			t.Fatal("want an error: EVENT_PUBLISHER=kafka needs KAFKA_BROKERS")
		}
	})
	t.Run("unknown publisher", func(t *testing.T) {
		t.Setenv("EVENT_PUBLISHER", "nope")
		if _, _, err := startOutboxRelay(countingStore{}, quietLogger()); err == nil {
			t.Fatal("want an error for an unknown EVENT_PUBLISHER")
		}
	})
}

func TestBuildAdapters_InMemoryWiresPhase4Ports(t *testing.T) {
	ad, closeFn, err := buildAdapters(context.Background(), "", defaultMigrationsPath, quietLogger())
	if err != nil {
		t.Fatalf("buildAdapters: %v", err)
	}
	defer closeFn()
	if ad.capacityPlans == nil || ad.outbox == nil || ad.outboxStore == nil || ad.uow == nil {
		t.Fatalf("Phase 4 ports not wired: %+v", ad)
	}
}

func TestMigrationsDatabaseURL(t *testing.T) {
	const pooled = "postgres://u@pgbouncer.example:6432/db"
	const direct = "postgres://u@postgres.example:5432/db"

	t.Setenv("MIGRATIONS_DATABASE_URL", "")
	if got := migrationsDatabaseURL(pooled); got != pooled {
		t.Errorf("unset MIGRATIONS_DATABASE_URL: got %q, want the DATABASE_URL %q", got, pooled)
	}
	t.Setenv("MIGRATIONS_DATABASE_URL", direct)
	if got := migrationsDatabaseURL(pooled); got != direct {
		t.Errorf("set MIGRATIONS_DATABASE_URL: got %q, want the direct DSN %q", got, direct)
	}
}
