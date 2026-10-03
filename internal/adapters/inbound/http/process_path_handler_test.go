package http_test

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
)

func registerProcessPath(t *testing.T, srv *httptest.Server, id, name string, steps []string) (int, http.Header, []byte) {
	t.Helper()
	return postJSON(t, srv, "/process-paths", map[string]any{
		"id":    id,
		"name":  name,
		"steps": steps,
	})
}

func getProcessPathCapacity(t *testing.T, srv *httptest.Server, id, location, windowStart, windowEnd string, unitsPerOrder, packagesPerOrder *float64) (int, http.Header, []byte) {
	t.Helper()
	path := fmt.Sprintf("/process-paths/%s/capacity?location=%s&window_start=%s&window_end=%s", id, location, windowStart, windowEnd)
	if unitsPerOrder != nil {
		path += fmt.Sprintf("&units_per_order=%v", *unitsPerOrder)
	}
	if packagesPerOrder != nil {
		path += fmt.Sprintf("&packages_per_order=%v", *packagesPerOrder)
	}
	return getPath(t, srv, path)
}

func registerPickRebinPackCapacities(t *testing.T, srv *httptest.Server, location string) {
	t.Helper()
	registrations := []struct {
		processType string
		quantity    float64
		unit        string
	}{
		{"PICK", 4000, "UNIT"},
		{"REBIN", 2500, "UNIT"},
		{"PACK", 1800, "PACKAGE"},
	}
	for _, reg := range registrations {
		status, _, body := postJSON(t, srv, "/process-capacities", map[string]any{
			"process_type":    reg.processType,
			"location":        location,
			"window_start":    "2026-10-05T08:00:00Z",
			"window_end":      "2026-10-05T09:00:00Z",
			"constraint_type": "LABOR",
			"quantity":        reg.quantity,
			"unit":            reg.unit,
			"period_seconds":  3600,
		})
		if status != http.StatusCreated {
			t.Fatalf("registering %s: expected 201, got %d: %s", reg.processType, status, string(body))
		}
	}
}

// TestHandler_ProcessPathCapacity_WorkedExample reproduces, byte-for-byte,
// the design doc's Pick -> Rebin -> Pack worked example end to end through
// the real HTTP surface: register the three ProcessCapacities, register
// the ProcessPath, then assert the path-capacity endpoint reports 1000
// ORDER/HOUR with REBIN as the bottleneck.
func TestHandler_ProcessPathCapacity_WorkedExample(t *testing.T) {
	srv, _ := newTestServer()
	defer srv.Close()

	registerPickRebinPackCapacities(t, srv, "PATH-ZONE-A")

	status, _, body := registerProcessPath(t, srv, "pick-rebin-pack", "Pick-Rebin-Pack", []string{"PICK", "REBIN", "PACK"})
	if status != http.StatusCreated {
		t.Fatalf("expected 201 registering the process path, got %d: %s", status, string(body))
	}

	unitsPerOrder, packagesPerOrder := 2.5, 1.0
	status, _, body = getProcessPathCapacity(t, srv, "pick-rebin-pack", "PATH-ZONE-A", "2026-10-05T08:00:00Z", "2026-10-05T09:00:00Z", &unitsPerOrder, &packagesPerOrder)
	if status != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", status, string(body))
	}

	var got struct {
		NormalizedRate float64 `json:"normalized_rate"`
		NormalizedUnit string  `json:"normalized_unit"`
		BottleneckStep string  `json:"bottleneck_step"`
	}
	if err := json.Unmarshal(body, &got); err != nil {
		t.Fatalf("unexpected error decoding response: %v", err)
	}
	if got.NormalizedRate != 1000 || got.NormalizedUnit != "ORDER" || got.BottleneckStep != "REBIN" {
		t.Fatalf("expected 1000 ORDER bound by REBIN, got %+v", got)
	}
}

func TestHandler_ProcessPathCapacity_NotFoundWhenPathNeverRegistered(t *testing.T) {
	srv, _ := newTestServer()
	defer srv.Close()

	unitsPerOrder, packagesPerOrder := 2.5, 1.0
	status, headers, _ := getProcessPathCapacity(t, srv, "nowhere-path", "PATH-ZONE-A", "2026-01-01T00:00:00Z", "2026-01-01T01:00:00Z", &unitsPerOrder, &packagesPerOrder)
	if status != http.StatusNotFound {
		t.Fatalf("expected 404, got %d", status)
	}
	if ct := headers.Get("Content-Type"); ct != "application/problem+json" {
		t.Fatalf("expected application/problem+json, got %q", ct)
	}
}

func TestHandler_ProcessPathCapacity_UnprocessableWhenAStepHasNoCapacity(t *testing.T) {
	srv, _ := newTestServer()
	defer srv.Close()

	status, _, body := postJSON(t, srv, "/process-capacities", map[string]any{
		"process_type":    "PICK",
		"location":        "PATH-ZONE-B",
		"window_start":    "2026-10-05T08:00:00Z",
		"window_end":      "2026-10-05T09:00:00Z",
		"constraint_type": "LABOR",
		"quantity":        4000,
		"unit":            "UNIT",
		"period_seconds":  3600,
	})
	if status != http.StatusCreated {
		t.Fatalf("expected 201 registering PICK, got %d: %s", status, string(body))
	}

	status, _, body = registerProcessPath(t, srv, "pick-only", "Pick-Only", []string{"PICK", "REBIN"})
	if status != http.StatusCreated {
		t.Fatalf("expected 201 registering the process path, got %d: %s", status, string(body))
	}

	unitsPerOrder, packagesPerOrder := 2.5, 1.0
	status, _, body = getProcessPathCapacity(t, srv, "pick-only", "PATH-ZONE-B", "2026-10-05T08:00:00Z", "2026-10-05T09:00:00Z", &unitsPerOrder, &packagesPerOrder)
	if status != http.StatusUnprocessableEntity {
		t.Fatalf("expected 422, got %d: %s", status, string(body))
	}
}

func TestHandler_ProcessPath_RejectsEmptySteps(t *testing.T) {
	srv, _ := newTestServer()
	defer srv.Close()

	status, _, body := registerProcessPath(t, srv, "empty-path", "Empty", nil)
	if status != http.StatusUnprocessableEntity {
		t.Fatalf("expected 422 for an empty step list, got %d: %s", status, string(body))
	}
}
