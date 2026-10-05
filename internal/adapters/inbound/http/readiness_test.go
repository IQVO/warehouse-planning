package http

import (
	"net/http/httptest"
	"testing"
)

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
