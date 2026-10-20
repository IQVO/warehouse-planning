package http_test

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	inboundhttp "github.com/claudioed/warehouse-planning/internal/adapters/inbound/http"
	"github.com/claudioed/warehouse-planning/internal/adapters/outbound/analyticsstore"
)

// The reports binary is ready only while the analytical Postgres answers; the
// problem names it. /healthz stays liveness.
func TestReportsReadyzFollowsTheAnalyticalDatabase(t *testing.T) {
	var down bool
	gate := inboundhttp.NewReadiness(1, 0, inboundhttp.Dependency{Name: "analytics-postgres", Check: func(context.Context) error {
		if down {
			return errors.New("connection refused")
		}
		return nil
	}})
	h := inboundhttp.NewReportsRouter(&inboundhttp.ReportsServer{Reader: analyticsstore.NewMemory(), Readiness: gate})
	readyz := func() *httptest.ResponseRecorder {
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/readyz", http.NoBody))
		return rec
	}

	if rec := readyz(); rec.Code != http.StatusOK {
		t.Fatalf("readyz with the database up = %d %s, want 200", rec.Code, rec.Body)
	}
	down = true
	rec := readyz() // the 1ns ttl has long expired
	if rec.Code != http.StatusServiceUnavailable || !strings.Contains(rec.Body.String(), "not ready: analytics-postgres") {
		t.Fatalf("readyz with the database down = %d %s, want 503 naming it", rec.Code, rec.Body)
	}
	if rec := get(t, h, "/healthz"); rec.Code != http.StatusOK {
		t.Fatalf("healthz = %d, want 200", rec.Code)
	}
	gate.SetNotReady()
	down = false
	if rec := readyz(); rec.Code != http.StatusServiceUnavailable || !strings.Contains(rec.Body.String(), "not_ready") {
		t.Fatalf("readyz while draining = %d %s", rec.Code, rec.Body)
	}
}
