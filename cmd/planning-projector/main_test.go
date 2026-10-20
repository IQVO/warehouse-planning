package main

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"testing"

	inboundhttp "github.com/claudioed/warehouse-planning/internal/adapters/inbound/http"
)

func env(m map[string]string) func(string) string { return func(k string) string { return m[k] } }

func TestLoadConfig_Defaults(t *testing.T) {
	c, err := loadConfig(env(map[string]string{"ANALYTICS_DATABASE_URL": "postgres://u@h/db", "KAFKA_BROKERS": "k1:9092, k2:9092,,", "ANALYTICS_CONSUMER_GROUP": "warehouse-planning-analytics"}))
	if err != nil {
		t.Fatal(err)
	}
	want := config{
		analyticsURL: "postgres://u@h/db", brokers: []string{"k1:9092", "k2:9092"},
		group: "warehouse-planning-analytics", migrationsPath: "analytics/migrations", adminAddr: ":8091", logLevel: "info",
	}
	if !reflect.DeepEqual(c, want) {
		t.Fatalf("config = %+v, want %+v", c, want)
	}
}

func TestLoadConfig_EnvironmentOverridesEverything(t *testing.T) {
	c, err := loadConfig(env(map[string]string{
		"ANALYTICS_DATABASE_URL": "postgres://u@h/db", "KAFKA_BROKERS": "kafka:9092",
		"ANALYTICS_CONSUMER_GROUP": "g-from-env", "ANALYTICS_MIGRATIONS_PATH": "/app/analytics/migrations",
		"ADMIN_ADDR": ":9999", "LOG_LEVEL": "debug",
	}))
	if err != nil {
		t.Fatal(err)
	}
	if c.group != "g-from-env" || c.migrationsPath != "/app/analytics/migrations" || c.adminAddr != ":9999" || c.logLevel != "debug" {
		t.Fatalf("config = %+v", c)
	}
}

// Every mandatory value is a boot error that names its variable; the consumer
// group has NO default (a default would let a local process join the live group).
func TestLoadConfig_RequiresTheAnalyticalDatabaseKafkaAndGroup(t *testing.T) {
	full := map[string]string{"ANALYTICS_DATABASE_URL": "postgres://u@h/db", "KAFKA_BROKERS": "k:9092", "ANALYTICS_CONSUMER_GROUP": "g"}
	cases := []struct {
		drop string
		want error
	}{
		{"ANALYTICS_DATABASE_URL", errMissingAnalyticsURL},
		{"KAFKA_BROKERS", errMissingBrokers},
		{"ANALYTICS_CONSUMER_GROUP", errMissingGroup},
	}
	for _, tc := range cases {
		m := map[string]string{}
		for k, v := range full {
			if k != tc.drop {
				m[k] = v
			}
		}
		_, err := loadConfig(env(m))
		if !errors.Is(err, tc.want) || !strings.Contains(err.Error(), tc.drop) {
			t.Errorf("without %s: err = %v, want %v naming the variable", tc.drop, err, tc.want)
		}
	}
	blank := map[string]string{"ANALYTICS_DATABASE_URL": "postgres://u@h/db", "KAFKA_BROKERS": " , ", "ANALYTICS_CONSUMER_GROUP": "g"}
	if _, err := loadConfig(env(blank)); !errors.Is(err, errMissingBrokers) {
		t.Errorf("blank brokers: err = %v", err)
	}
}

func TestRun_FailsFastWithoutTheAnalyticalDatabase(t *testing.T) {
	t.Setenv("ANALYTICS_DATABASE_URL", "")
	t.Setenv("KAFKA_BROKERS", "k:9092")
	if err := run(); !errors.Is(err, errMissingAnalyticsURL) {
		t.Fatalf("run = %v", err)
	}
}

func TestAdminMux_HealthzIsLivenessAndReadyzFlipsOnShutdown(t *testing.T) {
	readiness := inboundhttp.NewReadiness(0, 0) // no dependency: the drain gate alone
	mux := newAdminMux(readiness)
	get := func(path string) *httptest.ResponseRecorder {
		rec := httptest.NewRecorder()
		mux.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, path, nil))
		return rec
	}
	if rec := get("/healthz"); rec.Code != 200 || rec.Body.String() != `{"status":"ok"}` {
		t.Fatalf("healthz = %d %s", rec.Code, rec.Body)
	}
	if rec := get("/readyz"); rec.Code != 200 || strings.TrimSpace(rec.Body.String()) != `{"status":"ready"}` {
		t.Fatalf("readyz = %d %s", rec.Code, rec.Body)
	}
	readiness.SetNotReady()
	if rec := get("/readyz"); rec.Code != 503 || strings.TrimSpace(rec.Body.String()) != `{"status":"not_ready"}` {
		t.Fatalf("readyz during shutdown = %d %s", rec.Code, rec.Body)
	}
	if rec := get("/healthz"); rec.Code != 200 {
		t.Fatalf("healthz must stay up during shutdown, got %d", rec.Code)
	}
}

// The real gate: the analytical Postgres and Kafka are both named when down,
// and healthz stays up.
func TestAdminMux_ReadyzReportsDownDependencies(t *testing.T) {
	down := func(context.Context) error { return errors.New("connection refused") }
	mux := newAdminMux(newReadiness(down, []string{"127.0.0.1:1"})) // nothing listens on port 1
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/readyz", nil))
	if rec.Code != http.StatusServiceUnavailable || !strings.Contains(rec.Body.String(), "not ready: analytics-postgres, kafka") {
		t.Fatalf("readyz = %d %s, want 503 naming analytics-postgres and kafka", rec.Code, rec.Body)
	}
	rec = httptest.NewRecorder()
	mux.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/healthz", nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("healthz = %d, want 200 (liveness ignores dependencies)", rec.Code)
	}
}

func TestNewLogger_LevelMapping(t *testing.T) {
	for level, enabledDebug := range map[string]bool{"debug": true, "info": false, "": false, "nonsense": false} {
		if got := newLogger(level).Enabled(context.Background(), -4); got != enabledDebug {
			t.Errorf("level %q: debug enabled = %v, want %v", level, got, enabledDebug)
		}
	}
	if newLogger("warn").Enabled(context.Background(), 0) || !newLogger("warning").Enabled(context.Background(), 4) || newLogger("error").Enabled(context.Background(), 4) {
		t.Error("warn/error level mapping is wrong")
	}
}
