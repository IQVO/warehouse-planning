package http_test

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/claudioed/warehouse-planning/internal/adapters/outbound/memory"
	"github.com/claudioed/warehouse-planning/internal/domain/processflow"
)

// seedFlowFor records the ACTIVE process flow a path id resolves to (ADR 0025
// stage 3: flows are the only source); each stage's path is its own process
// type, the shape the producer's examples use.
func seedFlowFor(t *testing.T, flows *memory.ProcessFlowRepo, id, name string, steps []string) {
	t.Helper()
	stages := make([]processflow.Stage, 0, len(steps))
	for _, s := range steps {
		stages = append(stages, processflow.Stage{PathID: s, ProcessType: processflow.ProcessType(s)})
	}
	f, err := processflow.New(processflow.Params{FlowID: id, Name: name, Stages: stages, Status: processflow.StatusActive, Version: 1, UpdatedAt: time.Unix(0, 0).UTC()})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := flows.Record(context.Background(), f); err != nil {
		t.Fatal(err)
	}
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
	srv, _, flows := newTestServer()
	defer srv.Close()

	registerPickRebinPackCapacities(t, srv, "PATH-ZONE-A")

	seedFlowFor(t, flows, "pick-rebin-pack", "Pick-Rebin-Pack", []string{"PICK", "REBIN", "PACK"})

	unitsPerOrder, packagesPerOrder := 2.5, 1.0
	status, _, body := getProcessPathCapacity(t, srv, "pick-rebin-pack", "PATH-ZONE-A", "2026-10-05T08:00:00Z", "2026-10-05T09:00:00Z", &unitsPerOrder, &packagesPerOrder)
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
	srv, _, _ := newTestServer()
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
	srv, _, flows := newTestServer()
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

	seedFlowFor(t, flows, "pick-only", "Pick-Only", []string{"PICK", "REBIN"})

	unitsPerOrder, packagesPerOrder := 2.5, 1.0
	status, _, body = getProcessPathCapacity(t, srv, "pick-only", "PATH-ZONE-B", "2026-10-05T08:00:00Z", "2026-10-05T09:00:00Z", &unitsPerOrder, &packagesPerOrder)
	if status != http.StatusUnprocessableEntity {
		t.Fatalf("expected 422, got %d: %s", status, string(body))
	}
}

// TestHandler_ProcessPath_RegisterIs410 pins ADR 0025 stage 2: the local
// declaration retired, so POST /process-paths answers 410 process-path-moved
// for ANY body (nothing decoded, recorded or published), naming
// process-path-management's PUT /process-flows/{flowId}.
func TestHandler_ProcessPath_RegisterIs410(t *testing.T) {
	srv, _, _ := newTestServer()
	defer srv.Close()

	for _, body := range []string{`{"id":"x","name":"X","steps":["PICK"]}`, `not json`, ``} {
		req, err := http.NewRequest(http.MethodPost, srv.URL+"/process-paths", strings.NewReader(body))
		if err != nil {
			t.Fatal(err)
		}
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Idempotency-Key", "retire-"+body)
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		b, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		if resp.StatusCode != http.StatusGone {
			t.Fatalf("body %q: expected 410, got %d: %s", body, resp.StatusCode, string(b))
		}
		if ct := resp.Header.Get("Content-Type"); ct != "application/problem+json" {
			t.Fatalf("expected application/problem+json, got %q", ct)
		}
		var problem struct {
			Type string `json:"type"`
		}
		if err := json.Unmarshal(b, &problem); err != nil {
			t.Fatal(err)
		}
		if !strings.HasSuffix(problem.Type, "/process-path-moved") {
			t.Fatalf("expected process-path-moved, got %s", problem.Type)
		}
	}
}
