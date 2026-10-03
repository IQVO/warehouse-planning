package http_test

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	inboundhttp "github.com/claudioed/warehouse-planning/internal/adapters/inbound/http"
	"github.com/claudioed/warehouse-planning/internal/adapters/outbound/memory"
	"github.com/claudioed/warehouse-planning/internal/application/usecases"
)

func newTestServer() (*httptest.Server, *memory.ProcessCapacityRepo) {
	repo := memory.NewProcessCapacityRepo()
	s := &inboundhttp.Server{
		RegisterProcessCapacityConstraint: &usecases.RegisterProcessCapacityConstraint{Repo: repo},
		ProcessCapacities:                 repo,
	}
	return httptest.NewServer(inboundhttp.NewRouter(s)), repo
}

// doRequest issues req and returns the status code, headers and fully
// drained body, closing the response body itself so no call site needs
// its own defer resp.Body.Close().
func doRequest(t *testing.T, req *http.Request) (int, http.Header, []byte) {
	t.Helper()
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("unexpected error issuing request: %v", err)
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("unexpected error reading response body: %v", err)
	}
	return resp.StatusCode, resp.Header, body
}

func postJSON(t *testing.T, srv *httptest.Server, path string, payload map[string]any) (int, http.Header, []byte) {
	t.Helper()
	encoded, err := json.Marshal(payload)
	if err != nil {
		t.Fatalf("unexpected error marshaling request: %v", err)
	}
	req, err := http.NewRequest(http.MethodPost, srv.URL+path, bytes.NewReader(encoded))
	if err != nil {
		t.Fatalf("unexpected error building request: %v", err)
	}
	req.Header.Set("Content-Type", "application/json")
	return doRequest(t, req)
}

func getPath(t *testing.T, srv *httptest.Server, path string) (int, http.Header, []byte) {
	t.Helper()
	req, err := http.NewRequest(http.MethodGet, srv.URL+path, nil)
	if err != nil {
		t.Fatalf("unexpected error building request: %v", err)
	}
	return doRequest(t, req)
}

func registerConstraint(t *testing.T, srv *httptest.Server, constraintType string, quantity float64) (int, http.Header, []byte) {
	t.Helper()
	return postJSON(t, srv, "/process-capacities", map[string]any{
		"process_type":    "PICK",
		"location":        "PICK-ZONE-A",
		"window_start":    "2026-10-05T08:00:00Z",
		"window_end":      "2026-10-05T09:00:00Z",
		"constraint_type": constraintType,
		"quantity":        quantity,
		"unit":            "UNIT",
		"period_seconds":  3600,
	})
}

// TestHandler_WorkedExample reproduces the PICK/PICK-ZONE-A worked example
// end to end through the real HTTP surface: register four constraints,
// then assert GET reports 3500 UNIT/HOUR with LOCATION binding.
func TestHandler_WorkedExample(t *testing.T) {
	srv, _ := newTestServer()
	defer srv.Close()

	registrations := []struct {
		constraintType string
		quantity       float64
	}{
		{"LABOR", 4000},
		{"LOCATION", 3500},
		{"EQUIPMENT", 5000},
		{"CONVEYOR", 3800},
	}

	var lastBody map[string]any
	for _, reg := range registrations {
		status, _, body := registerConstraint(t, srv, reg.constraintType, reg.quantity)
		if status != http.StatusCreated {
			t.Fatalf("registering %s: expected 201, got %d: %s", reg.constraintType, status, string(body))
		}
		if err := json.Unmarshal(body, &lastBody); err != nil {
			t.Fatalf("unexpected error decoding response: %v", err)
		}
	}

	if lastBody["binding_constraint"] != "LOCATION" {
		t.Fatalf("expected the final POST's binding_constraint to be LOCATION, got %v", lastBody["binding_constraint"])
	}
	if lastBody["effective_rate"] != 3500.0 {
		t.Fatalf("expected the final POST's effective_rate to be 3500, got %v", lastBody["effective_rate"])
	}

	status, _, body := getPath(t, srv, "/process-capacities?process_type=PICK&location=PICK-ZONE-A&window_start=2026-10-05T08:00:00Z&window_end=2026-10-05T09:00:00Z")
	if status != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", status, string(body))
	}

	var got struct {
		EffectiveRate     float64 `json:"effective_rate"`
		EffectiveUnit     string  `json:"effective_unit"`
		BindingConstraint string  `json:"binding_constraint"`
		Constraints       []struct {
			ConstraintType string  `json:"constraint_type"`
			Quantity       float64 `json:"quantity"`
		} `json:"constraints"`
	}
	if err := json.Unmarshal(body, &got); err != nil {
		t.Fatalf("unexpected error decoding GET response: %v", err)
	}
	if got.EffectiveRate != 3500 || got.EffectiveUnit != "UNIT" || got.BindingConstraint != "LOCATION" {
		t.Fatalf("expected 3500 UNIT with LOCATION binding, got %+v", got)
	}
	if len(got.Constraints) != 4 {
		t.Fatalf("expected 4 constraints in the GET response, got %d", len(got.Constraints))
	}
}

func TestHandler_Get_NotFoundWhenNothingRegistered(t *testing.T) {
	srv, _ := newTestServer()
	defer srv.Close()

	status, headers, _ := getPath(t, srv, "/process-capacities?process_type=PACK&location=NOWHERE&window_start=2026-01-01T00:00:00Z&window_end=2026-01-01T01:00:00Z")
	if status != http.StatusNotFound {
		t.Fatalf("expected 404, got %d", status)
	}
	if ct := headers.Get("Content-Type"); ct != "application/problem+json" {
		t.Fatalf("expected application/problem+json, got %q", ct)
	}
}

func TestHandler_Post_RejectsMismatchedUnit(t *testing.T) {
	srv, _ := newTestServer()
	defer srv.Close()

	status, _, body := registerConstraint(t, srv, "LABOR", 4000)
	if status != http.StatusCreated {
		t.Fatalf("expected 201 for first registration, got %d: %s", status, string(body))
	}

	status2, _, body2 := postJSON(t, srv, "/process-capacities", map[string]any{
		"process_type":    "PICK",
		"location":        "PICK-ZONE-A",
		"window_start":    "2026-10-05T08:00:00Z",
		"window_end":      "2026-10-05T09:00:00Z",
		"constraint_type": "EQUIPMENT",
		"quantity":        50,
		"unit":            "PACKAGE",
		"period_seconds":  3600,
	})
	if status2 != http.StatusConflict {
		t.Fatalf("expected 409 for a mismatched unit, got %d: %s", status2, string(body2))
	}
}

func TestHandler_Post_RejectsMalformedJSON(t *testing.T) {
	srv, _ := newTestServer()
	defer srv.Close()

	req, err := http.NewRequest(http.MethodPost, srv.URL+"/process-capacities", bytes.NewReader([]byte("{not json")))
	if err != nil {
		t.Fatalf("unexpected error building request: %v", err)
	}
	req.Header.Set("Content-Type", "application/json")
	status, _, _ := doRequest(t, req)
	if status != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", status)
	}
}

func TestHandler_Post_RejectsInvalidWindow(t *testing.T) {
	srv, _ := newTestServer()
	defer srv.Close()

	status, _, body := postJSON(t, srv, "/process-capacities", map[string]any{
		"process_type":    "PICK",
		"location":        "PICK-ZONE-A",
		"window_start":    "2026-10-05T09:00:00Z",
		"window_end":      "2026-10-05T08:00:00Z",
		"constraint_type": "LABOR",
		"quantity":        4000,
		"unit":            "UNIT",
		"period_seconds":  3600,
	})
	if status != http.StatusBadRequest {
		t.Fatalf("expected 400 for an inverted window, got %d: %s", status, string(body))
	}
}

func TestHandler_Healthz(t *testing.T) {
	srv, _ := newTestServer()
	defer srv.Close()

	status, _, _ := getPath(t, srv, "/healthz")
	if status != http.StatusOK {
		t.Fatalf("expected 200, got %d", status)
	}
}
