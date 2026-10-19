package main

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"strings"
	"testing"
	"time"

	outboxrelay "github.com/claudioed/warehouse-planning/internal/adapters/outbound/outbox"
	"github.com/claudioed/warehouse-planning/internal/application/outbox"
)

func quietLogger() *slog.Logger { return slog.New(slog.NewTextHandler(io.Discard, nil)) }

// validEnv is a complete real-runtime environment; each test case removes or
// breaks one variable.
func validEnv() map[string]string {
	return map[string]string{
		"DATABASE_URL":                    "postgres://u@db.invalid:5432/d",
		"KAFKA_BROKERS":                   "kafka-1.invalid:9092, kafka-2.invalid:9092",
		"LABOR_CAPACITY_CONSUMER_GROUP":   "g-labor",
		"STORAGE_CAPACITY_CONSUMER_GROUP": "g-storage",
		"DEMAND_CONSUMER_GROUP":           "g-demand",
		"DEMAND_SITE_ID":                  "SIM1",
	}
}

func envFunc(m map[string]string) func(string) string {
	return func(k string) string { return m[k] }
}

func TestLoadRuntime_AcceptsACompleteEnvironment(t *testing.T) {
	rt, err := loadRuntime(envFunc(validEnv()))
	if err != nil {
		t.Fatalf("loadRuntime: %v", err)
	}
	if len(rt.brokers) != 2 || rt.brokers[1] != "kafka-2.invalid:9092" {
		t.Errorf("brokers = %v, want two trimmed addresses", rt.brokers)
	}
	if rt.groups["LABOR_CAPACITY_CONSUMER_GROUP"] != "g-labor" || rt.groups["STORAGE_CAPACITY_CONSUMER_GROUP"] != "g-storage" || rt.groups["DEMAND_CONSUMER_GROUP"] != "g-demand" {
		t.Errorf("groups = %v", rt.groups)
	}
	if rt.demandSiteID != "SIM1" {
		t.Errorf("demandSiteID = %q", rt.demandSiteID)
	}
}

func TestLoadRuntime_EventPublisherKafkaIsAccepted(t *testing.T) {
	env := validEnv()
	env["EVENT_PUBLISHER"] = "kafka"
	if _, err := loadRuntime(envFunc(env)); err != nil {
		t.Fatalf("EVENT_PUBLISHER=kafka: %v", err)
	}
}

// Every mandatory or rejected value is a boot error that NAMES the variable.
func TestLoadRuntime_BootErrorsNameTheVariable(t *testing.T) {
	cases := []struct {
		name     string
		mutate   func(map[string]string)
		wantName string
	}{
		{"no database", func(e map[string]string) { delete(e, "DATABASE_URL") }, "DATABASE_URL"},
		{"no brokers", func(e map[string]string) { delete(e, "KAFKA_BROKERS") }, "KAFKA_BROKERS"},
		{"blank brokers", func(e map[string]string) { e["KAFKA_BROKERS"] = " , " }, "KAFKA_BROKERS"},
		{"log publisher", func(e map[string]string) { e["EVENT_PUBLISHER"] = "log" }, "EVENT_PUBLISHER"},
		{"unknown publisher", func(e map[string]string) { e["EVENT_PUBLISHER"] = "rabbit" }, "EVENT_PUBLISHER"},
		{"no labor group", func(e map[string]string) { delete(e, "LABOR_CAPACITY_CONSUMER_GROUP") }, "LABOR_CAPACITY_CONSUMER_GROUP"},
		{"no storage group", func(e map[string]string) { delete(e, "STORAGE_CAPACITY_CONSUMER_GROUP") }, "STORAGE_CAPACITY_CONSUMER_GROUP"},
		{"no demand group", func(e map[string]string) { delete(e, "DEMAND_CONSUMER_GROUP") }, "DEMAND_CONSUMER_GROUP"},
		{"no demand site", func(e map[string]string) { delete(e, "DEMAND_SITE_ID") }, "DEMAND_SITE_ID"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			env := validEnv()
			tc.mutate(env)
			_, err := loadRuntime(envFunc(env))
			if err == nil {
				t.Fatalf("want a boot error naming %s", tc.wantName)
			}
			if !strings.Contains(err.Error(), tc.wantName) {
				t.Fatalf("error %q does not name %s", err, tc.wantName)
			}
		})
	}
}

func TestParsePublisherMode(t *testing.T) {
	for _, raw := range []string{"", "kafka"} {
		if err := parsePublisherMode(raw); err != nil {
			t.Errorf("parsePublisherMode(%q) = %v, want nil", raw, err)
		}
	}
	err := parsePublisherMode("log")
	if err == nil || !strings.Contains(err.Error(), "EVENT_PUBLISHER=log is not supported") {
		t.Errorf("log must be refused naming the variable, got %v", err)
	}
	if err := parsePublisherMode("rabbit"); err == nil {
		t.Error("an unknown EVENT_PUBLISHER must be a config error")
	}
}

func TestBuildAdapters_RefusesAnEmptyDatabaseURL(t *testing.T) {
	_, _, err := buildAdapters(context.Background(), "", defaultMigrationsPath, quietLogger())
	if !errors.Is(err, errMissingDatabaseURL) {
		t.Fatalf("buildAdapters(\"\") = %v, want errMissingDatabaseURL", err)
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

func (s countingStore) Drain(ctx context.Context, _, _ int, _ func(context.Context, outbox.Message) error) (int, error) {
	select {
	case s.drains <- struct{}{}:
	default:
	}
	return 0, ctx.Err()
}

var _ outboxrelay.Store = countingStore{}

// A broker that is down at boot must not crash or block startup: the Kafka
// writer is constructed but never dialled until the relay's first send.
func TestStartOutboxRelay_DoesNotDialAtBoot(t *testing.T) {
	t.Setenv("OUTBOX_RELAY_INTERVAL", "10ms")
	start := time.Now()
	w, stop, err := startOutboxRelay(countingStore{drains: make(chan struct{}, 1)}, []string{"127.0.0.1:1"}, quietLogger()) // nothing listens here
	if err != nil {
		t.Fatalf("startOutboxRelay with an unreachable broker: %v", err)
	}
	if elapsed := time.Since(start); elapsed > time.Second {
		t.Errorf("startup took %v; it must not wait on Kafka", elapsed)
	}
	stop()
	select {
	case <-w.done:
	case <-time.After(5 * time.Second):
		t.Fatal("the relay did not stop")
	}
}

func TestStartOutboxRelay_DrainsAndStops(t *testing.T) {
	t.Setenv("OUTBOX_RELAY_INTERVAL", "10ms")
	store := countingStore{drains: make(chan struct{}, 1)}
	w, stop, err := startOutboxRelay(store, []string{"127.0.0.1:1"}, quietLogger())
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
	case <-w.done:
	case <-time.After(5 * time.Second):
		t.Fatal("the relay did not stop")
	}
}

func TestStartOutboxRelay_RefusesNoBrokers(t *testing.T) {
	if _, _, err := startOutboxRelay(countingStore{}, nil, quietLogger()); !errors.Is(err, errMissingBrokers) {
		t.Fatalf("want errMissingBrokers, got %v", err)
	}
}

// A worker that returns on its own is reported by name.
func TestWatchWorkers_ReportsAWorkerThatReturns(t *testing.T) {
	alive := &worker{name: "alive", done: make(chan struct{})}
	dead := &worker{name: "dead", done: make(chan struct{})}
	died := watchWorkers(nil, alive, dead)
	close(dead.done)
	select {
	case name := <-died:
		if name != "dead" {
			t.Fatalf("died = %q, want dead", name)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("a worker that returned was not reported")
	}
}

func TestAwait_IsBoundedAndNilSafe(t *testing.T) {
	await(quietLogger(), nil)
	w := &worker{name: "w", done: make(chan struct{})}
	close(w.done)
	await(quietLogger(), w)
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
