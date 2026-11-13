//go:build integration

package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"runtime"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/testcontainers/testcontainers-go"
	tcpostgres "github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/wait"

	"github.com/claudioed/warehouse-planning/internal/adapters/outbound/analyticsstore"
	"github.com/claudioed/warehouse-planning/internal/adapters/outbound/postgres"
	"github.com/claudioed/warehouse-planning/internal/analytics/report"
)

// The real binary against a real analytical Postgres started with
// testcontainers (never a skip-gate): the database is emptied and
// analytically migrated, the facts are seeded through the service's OWN
// analytics writer (the same Projection the projector feeds), run() boots
// read-only over it, every documented endpoint answers with the shaped
// aggregates of ADR 0005 §5 (plus its documented 400 on an inverted range)
// and a SIGTERM drains the server cleanly.

var (
	sharedMu        sync.Mutex
	sharedPostgres  testcontainers.Container
	sharedAnalytics string
)

func TestMain(m *testing.M) {
	code := m.Run()
	sharedMu.Lock()
	postgresC := sharedPostgres
	sharedMu.Unlock()
	if postgresC != nil {
		if err := testcontainers.TerminateContainer(postgresC); err != nil {
			fmt.Fprintf(os.Stderr, "terminate postgres container: %v\n", err)
		}
	}
	os.Exit(code)
}

// startAnalyticsPostgres boots one fresh analytical Postgres per test binary
// and applies ONLY the analytics migrations: the reports binary never
// migrates (it is the read-only reader), so the test does what the projector
// did before it.
func startAnalyticsPostgres(t *testing.T) string {
	t.Helper()
	sharedMu.Lock()
	defer sharedMu.Unlock()
	if sharedAnalytics != "" {
		return sharedAnalytics
	}
	ctx := context.Background()
	c, err := tcpostgres.Run(ctx, "postgres:16-alpine",
		tcpostgres.WithDatabase("warehouse_planning_analytics"),
		tcpostgres.WithUsername("itest"), tcpostgres.WithPassword("itest"),
		testcontainers.WithWaitStrategy(wait.ForLog("database system is ready to accept connections").WithOccurrence(2).WithStartupTimeout(60*time.Second)))
	if err != nil {
		t.Fatalf("start analytics postgres: %v", err)
	}
	sharedPostgres = c
	url, err := c.ConnectionString(ctx, "sslmode=disable")
	if err != nil {
		t.Fatalf("analytics dsn: %v", err)
	}
	_, thisFile, _, _ := runtime.Caller(0)
	if err := postgres.RunMigrations(url, filepath.Join(filepath.Dir(thisFile), "..", "..", "analytics", "migrations")); err != nil {
		t.Fatalf("apply the analytics migrations: %v", err)
	}
	sharedAnalytics = url
	return url
}

func freeAddr(t *testing.T) string {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = l.Close() }()
	return l.Addr().String()
}

// ownSignal keeps a SIGTERM handler registered from before run() starts (so
// the stop signal can never kill the test binary in the window before
// run()'s signal.NotifyContext attaches) and stopWithSIGTERM delivers it
// repeatedly until run() returns.
func ownSignal() chan os.Signal {
	sig := make(chan os.Signal, 8)
	signal.Notify(sig, syscall.SIGTERM)
	return sig
}

func stopWithSIGTERM(t *testing.T, done <-chan error) error {
	t.Helper()
	for deadline := time.Now().Add(30 * time.Second); time.Now().Before(deadline); time.Sleep(200 * time.Millisecond) {
		_ = syscall.Kill(os.Getpid(), syscall.SIGTERM)
		select {
		case err := <-done:
			return err
		case <-time.After(300 * time.Millisecond):
		}
	}
	t.Fatal("the reports server did not stop after SIGTERM")
	return nil
}

func waitUntil(t *testing.T, what string, cond func() bool) {
	t.Helper()
	for deadline := time.Now().Add(30 * time.Second); time.Now().Before(deadline); time.Sleep(100 * time.Millisecond) {
		if cond() {
			return
		}
	}
	t.Fatalf("timed out waiting for %s", what)
}

func getJSON(t *testing.T, url string) (int, map[string]any) {
	t.Helper()
	resp, err := http.Get(url)
	if err != nil {
		return 0, nil
	}
	defer resp.Body.Close()
	var body map[string]any
	_ = json.NewDecoder(resp.Body).Decode(&body)
	return resp.StatusCode, body
}

// TestReportsRunServesTheSeededProjectionAndDrainsCleanly boots the real
// run() over a projection seeded through the repo's own writer and proves
// every documented endpoint plus the clean SIGTERM drain.
func TestReportsRunServesTheSeededProjectionAndDrainsCleanly(t *testing.T) {
	analyticsURL := startAnalyticsPostgres(t)
	ctx := context.Background()

	// Seed through the repo's own analytics writer: the same Projection
	// cmd/planning-projector feeds, folding the same event kinds. One plan,
	// created 08:00, published 08:10 (600 s latency), shortage 4000 bound
	// by LABOR on the REBIN step (the "section 43" worked example).
	created := time.Date(2026, 11, 13, 8, 0, 0, 0, time.UTC)
	published := created.Add(10 * time.Minute)
	const (
		planID      = "PLAN-REPORTS-ITEST-1"
		warehouseID = "WH-REPORTS-ITEST"
		location    = "PATH-ZONE-REPORTS"
	)
	writer, err := analyticsstore.NewPool(ctx, analyticsURL)
	if err != nil {
		t.Fatal(err)
	}
	defer writer.Close()
	str := func(s string) *string { return &s }
	num := func(f float64) *float64 { return &f }
	when := func(t time.Time) *time.Time { return &t }
	seed := []report.PlanEvent{
		{Kind: report.KindCreated, EventID: "evt-reports-1", At: created, PlanID: planID,
			WarehouseID: warehouseID, Location: location, CreatedAt: when(created)},
		{Kind: report.KindPublished, EventID: "evt-reports-2", At: published, PlanID: planID,
			WarehouseID: warehouseID, Location: location, BottleneckStep: str("REBIN"),
			BindingConstraint: str("LABOR"), PublishedAt: when(published)},
		{Kind: report.KindShortageDetected, EventID: "evt-reports-3", At: published, PlanID: planID,
			WarehouseID: warehouseID, Location: location, Shortage: num(4000)},
	}
	for _, e := range seed {
		applied, err := analyticsstore.NewProjection(writer).Apply(ctx, e)
		if err != nil || !applied {
			t.Fatalf("seed %s: applied=%v err=%v", e.Kind, applied, err)
		}
	}

	t.Setenv("ANALYTICS_DATABASE_URL", analyticsURL)
	t.Setenv("HTTP_ADDR", freeAddr(t))
	t.Setenv("LOG_LEVEL", "error")

	sig := ownSignal()
	defer signal.Stop(sig)
	done := make(chan error, 1)
	go func() { done <- run() }()

	base := "http://" + os.Getenv("HTTP_ADDR")
	// The documented probes: liveness is always 200; readiness is 200 only
	// while the analytical Postgres answers (the reports binary's one
	// dependency).
	waitUntil(t, "/healthz 200", func() bool {
		code, _ := getJSON(t, base+"/healthz")
		return code == http.StatusOK
	})
	waitUntil(t, "/readyz 200", func() bool {
		code, _ := getJSON(t, base+"/readyz")
		return code == http.StatusOK
	})

	rangeQ := "?from=2026-11-13T00:00:00Z&to=2026-11-14T00:00:00Z"

	// plan-throughput: one site-day, created 1 / published 1, and the
	// create-to-publish latency of the one published plan (600 s).
	code, body := getJSON(t, base+"/reports/plan-throughput"+rangeQ)
	days, _ := body["days"].([]any)
	if code != http.StatusOK || len(days) != 1 {
		t.Fatalf("plan-throughput = %d %v, want one day", code, body)
	}
	day := days[0].(map[string]any)
	if day["plans_created"] != 1.0 || day["plans_published"] != 1.0 || day["warehouse_id"] != warehouseID {
		t.Errorf("throughput day = %v, want 1 created / 1 published at %s", day, warehouseID)
	}
	latency, _ := body["latency"].([]any)
	if len(latency) != 1 {
		t.Fatalf("throughput latency = %v, want one site", body)
	}
	lat := latency[0].(map[string]any)
	if lat["plans"] != 1.0 || lat["median_seconds"] != 600.0 || lat["p95_seconds"] != 600.0 {
		t.Errorf("latency = %v, want 1 plan at median=p95=600 s", lat)
	}

	// bottleneck-frequency: the published plan's bottleneck step and the
	// binding constraint (the analytics-only payload addition).
	code, body = getJSON(t, base+"/reports/bottleneck-frequency"+rangeQ)
	rows, _ := body["rows"].([]any)
	if code != http.StatusOK || len(rows) != 1 {
		t.Fatalf("bottleneck-frequency = %d %v, want one row", code, body)
	}
	row := rows[0].(map[string]any)
	if row["bottleneck_step"] != "REBIN" || row["binding_constraint"] != "LABOR" ||
		row["plans"] != 1.0 || row["share"] != 1.0 {
		t.Errorf("bottleneck row = %v, want REBIN/LABOR plans=1 share=1", row)
	}

	// shortage-trend: the site-day of the published plan, shortage counted.
	code, body = getJSON(t, base+"/reports/shortage-trend"+rangeQ)
	sdays, _ := body["days"].([]any)
	if code != http.StatusOK || len(sdays) != 1 {
		t.Fatalf("shortage-trend = %d %v, want one day", code, body)
	}
	sday := sdays[0].(map[string]any)
	if sday["plans_published"] != 1.0 || sday["plans_with_shortage"] != 1.0 ||
		sday["total_shortage"] != 4000.0 || sday["shortage_rate"] != 1.0 {
		t.Errorf("shortage day = %v, want 1/1 published with shortage, total 4000, rate 1", sday)
	}

	// freshness: as_of is the newest applied CloudEvents time (the publish),
	// lag never negative.
	code, body = getJSON(t, base+"/reports/freshness")
	if code != http.StatusOK || body["as_of"] != published.Format(time.RFC3339Nano) {
		t.Fatalf("freshness = %d %v, want as_of %v", code, body, published)
	}
	if lag, _ := body["lag_seconds"].(float64); lag < 0 {
		t.Errorf("freshness lag = %v, never negative", lag)
	}

	// The documented parameter rule: an inverted range is a 400.
	if code, body := getJSON(t, base+"/reports/plan-throughput?from=2026-11-14T00:00:00Z&to=2026-11-13T00:00:00Z"); code != http.StatusBadRequest {
		t.Fatalf("inverted range = %d %v, want 400", code, body)
	}

	if err := stopWithSIGTERM(t, done); err != nil {
		t.Fatalf("a SIGTERM'd reports server must drain cleanly, got %v", err)
	}
}
