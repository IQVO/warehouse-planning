package http

import (
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestContainsNUL(t *testing.T) {
	tests := []struct {
		name string
		body string
		want bool
	}{
		{"plain", `{"a":"b"}`, false},
		{"raw NUL byte", "{\"a\":\"b\x00\"}", true},
		{"escape", `{"a":"b\u0000"}`, true},
		{"escape first", `{"\u0000":"b"}`, true},
		{"escaped backslash then text is not a NUL", `{"a":"b\\u0000"}`, false},
		{"three backslashes is an escaped backslash then an escape", `{"a":"b\\\u0000"}`, true},
		{"later real escape after a false one", `{"a":"\\u0000","b":"\u0000"}`, true},
		{"other control escapes are fine", `{"a":"\u0001\u001f"}`, false},
		{"empty", ``, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := containsNUL([]byte(tt.body)); got != tt.want {
				t.Errorf("containsNUL(%q) = %v, want %v", tt.body, got, tt.want)
			}
		})
	}
}

type nulCase struct {
	name       string
	method     string
	target     string
	body       string
	wantStatus int
	wantSlug   string
}

// serveNUL runs one request through rejectNUL and returns the recorder and
// the body the wrapped handler saw ("" when it was not reached).
func serveNUL(tt nulCase) (rec *httptest.ResponseRecorder, seen string, reached bool) {
	next := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		b, _ := io.ReadAll(r.Body)
		seen, reached = string(b), true
		w.WriteHeader(http.StatusNoContent)
	})
	var body io.Reader = http.NoBody
	if tt.body != "" {
		body = strings.NewReader(tt.body)
	}
	rec = httptest.NewRecorder()
	rejectNUL(next).ServeHTTP(rec, httptest.NewRequest(tt.method, tt.target, body))
	return rec, seen, reached
}

func TestRejectNULLetsCleanRequestsThrough(t *testing.T) {
	for _, tt := range []nulCase{
		{"clean POST reaches the handler with its body intact", http.MethodPost, "/capacity-plans", `{"location":"SIM1"}`, http.StatusNoContent, ""},
		{"clean GET", http.MethodGet, "/capacity-plans/plan-1?x=y", "", http.StatusNoContent, ""},
	} {
		t.Run(tt.name, func(t *testing.T) {
			rec, seen, reached := serveNUL(tt)
			if rec.Code != tt.wantStatus || !reached || seen != tt.body {
				t.Fatalf("status=%d reached=%v body=%q, want %d with body %q", rec.Code, reached, seen, tt.wantStatus, tt.body)
			}
		})
	}
}

// A body larger than the buffering cap is handed on whole.
func TestRejectNULKeepsABodyLargerThanTheBufferIntact(t *testing.T) {
	big := `{"a":"` + strings.Repeat("x", maxBodyBytes+10) + `"}`
	rec, seen, reached := serveNUL(nulCase{"big", http.MethodPost, "/capacity-plans", big, http.StatusNoContent, ""})
	if rec.Code != http.StatusNoContent || !reached || seen != big {
		t.Fatalf("status=%d reached=%v len(seen)=%d, want the whole %d bytes", rec.Code, reached, len(seen), len(big))
	}
}

func TestRejectNULRefusesBeforeTheHandler(t *testing.T) {
	for _, tt := range []nulCase{
		{"NUL escape in a JSON body is a 400", http.MethodPost, "/capacity-plans", `{"location":"a\u0000b"}`, http.StatusBadRequest, "malformed-request-body"},
		{"raw NUL in a JSON body is a 400", http.MethodPost, "/process-paths", "{\"id\":\"\x00\"}", http.StatusBadRequest, "malformed-request-body"},
		{"NUL in the path is a 404", http.MethodGet, "/capacity-plans/a%00b", "", http.StatusNotFound, "resource-not-found"},
		{"encoded NUL in the query is a 400", http.MethodGet, "/storage-capacity?location=a%00b", "", http.StatusBadRequest, "malformed-request-query"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			rec, _, reached := serveNUL(tt)
			if reached {
				t.Fatal("the handler was reached")
			}
			if rec.Code != tt.wantStatus {
				t.Fatalf("status = %d, want %d (%s)", rec.Code, tt.wantStatus, rec.Body.String())
			}
			if ct := rec.Header().Get("Content-Type"); ct != "application/problem+json" {
				t.Errorf("Content-Type = %q, want application/problem+json", ct)
			}
			if !strings.Contains(rec.Body.String(), problemBaseURI+tt.wantSlug) {
				t.Errorf("body lacks slug %q: %s", tt.wantSlug, rec.Body.String())
			}
		})
	}
}

// Through the real router the guard answers before any handler.
func TestRouterRejectsNULBeforeAnyHandler(t *testing.T) {
	router := NewRouter(&Server{})
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/capacity-plans", strings.NewReader(`{"location":"a\u0000"}`)))
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("POST with a NUL escape = %d %s, want 400", rec.Code, rec.Body)
	}
}
