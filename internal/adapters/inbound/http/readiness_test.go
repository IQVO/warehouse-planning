package http

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

// countingDep is a dependency whose verdict a test flips and whose checks it
// counts.
type countingDep struct {
	calls atomic.Int32
	fail  atomic.Bool
}

func (c *countingDep) dependency(name string) Dependency {
	return Dependency{Name: name, Check: func(context.Context) error {
		c.calls.Add(1)
		if c.fail.Load() {
			return context.DeadlineExceeded
		}
		return nil
	}}
}

func getReadyPath(g *Readiness, path string) *httptest.ResponseRecorder {
	rec := httptest.NewRecorder()
	NewRouter(&Server{Readiness: g}).ServeHTTP(rec, httptest.NewRequest(http.MethodGet, path, http.NoBody))
	return rec
}

func getReady(g *Readiness) *httptest.ResponseRecorder { return getReadyPath(g, "/readyz") }

// The dependency verdict is cached for the ttl so probes cannot hammer
// Postgres or Kafka; after the ttl the check runs again.
func TestReadinessCachesTheVerdictForTheTTL(t *testing.T) {
	probe := &countingDep{}
	gate := NewReadiness(time.Hour, time.Second, probe.dependency("postgres"))
	now := time.Unix(1_700_000_000, 0)
	gate.now = func() time.Time { return now }

	for range 5 {
		if failing := gate.Failing(context.Background()); failing != nil {
			t.Fatalf("failing = %v", failing)
		}
	}
	if n := probe.calls.Load(); n != 1 {
		t.Fatalf("checked %d times inside the ttl, want 1", n)
	}

	probe.fail.Store(true)
	if failing := gate.Failing(context.Background()); failing != nil {
		t.Fatalf("a cached verdict must not change inside the ttl, got %v", failing)
	}
	now = now.Add(time.Hour) // exactly the ttl: the verdict is stale
	if failing := gate.Failing(context.Background()); len(failing) != 1 || failing[0] != "postgres" {
		t.Fatalf("after the ttl the check must run again, got %v", failing)
	}
	if n := probe.calls.Load(); n != 2 {
		t.Fatalf("checked %d times, want 2", n)
	}
}

// A dependency that hangs is cut off by the timeout and reported down.
func TestReadinessBoundsASlowDependency(t *testing.T) {
	slow := Dependency{Name: "kafka", Check: func(ctx context.Context) error {
		<-ctx.Done()
		return ctx.Err()
	}}
	gate := NewReadiness(time.Second, 20*time.Millisecond, slow)
	start := time.Now()
	if failing := gate.Failing(context.Background()); len(failing) != 1 {
		t.Fatalf("failing = %v", failing)
	}
	if time.Since(start) > 2*time.Second {
		t.Fatal("the slow dependency was not cut off by the timeout")
	}
}

// A caller that hangs up must not poison the cached verdict.
func TestReadinessCheckIgnoresTheCallersCancellation(t *testing.T) {
	gate := NewReadiness(time.Hour, time.Second, Dependency{Name: "postgres", Check: func(ctx context.Context) error {
		return ctx.Err()
	}})
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if failing := gate.Failing(ctx); failing != nil {
		t.Fatalf("a cancelled caller context must not fail the dependency, got %v", failing)
	}
}

func TestReadinessDefaultsApplyToNonPositiveValues(t *testing.T) {
	gate := NewReadiness(0, -1)
	if gate.ttl != DefaultReadinessTTL || gate.timeout != DefaultReadinessTimeout {
		t.Fatalf("ttl=%v timeout=%v, want the defaults %v / %v", gate.ttl, gate.timeout, DefaultReadinessTTL, DefaultReadinessTimeout)
	}
	if DefaultReadinessTTL != 3*time.Second || DefaultReadinessTimeout != 2*time.Second {
		t.Fatalf("fleet bounds changed: ttl=%v timeout=%v, want 3s / 2s", DefaultReadinessTTL, DefaultReadinessTimeout)
	}
}

// Draining wins over the dependency verdict and does not run the checks.
func TestDrainingDoesNotCheckDependencies(t *testing.T) {
	probe := &countingDep{}
	gate := NewReadiness(time.Second, time.Second, probe.dependency("postgres"))
	gate.SetNotReady()
	rec := getReady(gate)
	if rec.Code != http.StatusServiceUnavailable || !strings.Contains(rec.Body.String(), "not_ready") || probe.calls.Load() != 0 {
		t.Fatalf("draining = %d %s (checks=%d)", rec.Code, rec.Body.String(), probe.calls.Load())
	}
}

// A failed dependency is a 503 problem+json naming the dependency; healthy
// dependencies are 200 ready.
func TestReadyzReportsAFailedDependencyAsAProblem(t *testing.T) {
	pg, kafka := &countingDep{}, &countingDep{}
	kafka.fail.Store(true)
	gate := NewReadiness(time.Nanosecond, time.Second, pg.dependency("postgres"), kafka.dependency("kafka"))

	rec := getReady(gate)
	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("readyz with Kafka down = %d %s, want 503", rec.Code, rec.Body)
	}
	if ct := rec.Header().Get("Content-Type"); ct != "application/problem+json" {
		t.Fatalf("content type = %q", ct)
	}
	body := rec.Body.String()
	for _, want := range []string{"dependency-unavailable", "not ready: kafka", "/readyz"} {
		if !strings.Contains(body, want) {
			t.Errorf("body lacks %q: %s", want, body)
		}
	}
	if strings.Contains(body, "postgres") {
		t.Errorf("healthy postgres must not be named: %s", body)
	}

	kafka.fail.Store(false)
	pg.fail.Store(true)
	time.Sleep(2 * time.Millisecond) // past the 1ns ttl
	if rec := getReady(gate); rec.Code != http.StatusServiceUnavailable || !strings.Contains(rec.Body.String(), "not ready: postgres") {
		t.Fatalf("readyz with Postgres down = %d %s", rec.Code, rec.Body)
	}

	pg.fail.Store(false)
	time.Sleep(2 * time.Millisecond)
	if rec := getReady(gate); rec.Code != http.StatusOK || !strings.Contains(rec.Body.String(), `"ready"`) {
		t.Fatalf("readyz with every dependency up = %d %s", rec.Code, rec.Body)
	}
}

// Liveness never depends on the dependencies.
func TestHealthzIgnoresDependencies(t *testing.T) {
	down := &countingDep{}
	down.fail.Store(true)
	rec := getReadyPath(NewReadiness(0, 0, down.dependency("postgres")), "/healthz")
	if rec.Code != http.StatusOK || down.calls.Load() != 0 {
		t.Fatalf("healthz = %d (checks=%d), want 200 without checking", rec.Code, down.calls.Load())
	}
}

func TestReadiness_ZeroValueIsReady(t *testing.T) {
	var r Readiness
	if !r.Ready() {
		t.Fatal("zero-value Readiness must report ready")
	}
}

func TestReadiness_NilIsReady(t *testing.T) {
	var r *Readiness
	if !r.Ready() {
		t.Fatal("a nil *Readiness must report ready (every pre-existing caller)")
	}
	r.SetNotReady() // must not panic
}

func TestReadiness_SetNotReadyFlipsAndStays(t *testing.T) {
	var r Readiness
	r.SetNotReady()
	if r.Ready() {
		t.Fatal("Ready() must report false after SetNotReady")
	}
	r.SetNotReady() // idempotent
	if r.Ready() {
		t.Fatal("still not ready after a second SetNotReady")
	}
}

func TestHandleReadyz_ReadyAndNotReady(t *testing.T) {
	s := &Server{}
	rec := httptest.NewRecorder()
	req := httptest.NewRequest("GET", "/readyz", nil)
	s.handleReadyz(rec, req)
	if rec.Code != 200 {
		t.Fatalf("nil Readiness: /readyz = %d, want 200", rec.Code)
	}

	s.Readiness = &Readiness{}
	s.Readiness.SetNotReady()
	rec = httptest.NewRecorder()
	s.handleReadyz(rec, req)
	if rec.Code != 503 {
		t.Fatalf("not-ready: /readyz = %d, want 503", rec.Code)
	}
}

// Through the real router: /readyz is registered and distinct from /healthz.
func TestNewRouter_ReadyzRegisteredAndDistinctFromHealthz(t *testing.T) {
	s := &Server{Readiness: &Readiness{}}
	router := NewRouter(s)

	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, httptest.NewRequest("GET", "/readyz", nil))
	if rec.Code != 200 {
		t.Fatalf("/readyz before SetNotReady = %d, want 200", rec.Code)
	}

	s.Readiness.SetNotReady()
	rec = httptest.NewRecorder()
	router.ServeHTTP(rec, httptest.NewRequest("GET", "/readyz", nil))
	if rec.Code != 503 {
		t.Fatalf("/readyz after SetNotReady = %d, want 503", rec.Code)
	}

	// /healthz (liveness) is never affected by readiness.
	rec = httptest.NewRecorder()
	router.ServeHTTP(rec, httptest.NewRequest("GET", "/healthz", nil))
	if rec.Code != 200 {
		t.Fatalf("/healthz after SetNotReady = %d, want 200 (liveness is never flipped)", rec.Code)
	}
}
