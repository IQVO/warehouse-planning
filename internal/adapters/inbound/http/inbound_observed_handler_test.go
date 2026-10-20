package http_test

import (
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	inboundhttp "github.com/claudioed/warehouse-planning/internal/adapters/inbound/http"
	"github.com/claudioed/warehouse-planning/internal/adapters/outbound/memory"
	"github.com/claudioed/warehouse-planning/internal/application/usecases"
	"github.com/claudioed/warehouse-planning/internal/domain/inbound"
)

// newInboundObservedServer wires the ADR 0017 routes over in-memory doubles.
func newInboundObservedServer(t *testing.T) (*httptest.Server, *memory.InboundAppointmentRepo) {
	t.Helper()
	appointments, standards := memory.NewInboundAppointmentRepo(), memory.NewInboundStandardRepo()
	s := &inboundhttp.Server{
		DeclareInboundLabourStandard: &usecases.DeclareInboundLabourStandard{Repo: standards},
		InboundStandards:             standards,
		GetInboundDemand:             &usecases.GetInboundDemand{Appointments: appointments, Standards: standards},
		GetCapacityPlanVariance: &usecases.GetCapacityPlanVariance{
			Plans: memory.NewCapacityPlanRepo(), Observed: memory.NewObservedCapacityRepo(),
		},
	}
	srv := httptest.NewServer(inboundhttp.NewRouter(s))
	t.Cleanup(srv.Close)
	return srv, appointments
}

// declareStandard PUTs an inbound labour standard and returns status + body.
func declareStandard(t *testing.T, srv *httptest.Server, path string, payload map[string]any) (int, []byte) {
	t.Helper()
	status, _, body := putJSON(t, srv, path, payload)
	return status, body
}

func TestInboundLabourStandard_DeclareCreatesThenReplaces(t *testing.T) {
	srv, _ := newInboundObservedServer(t)
	path := "/inbound-labour-standards/SIM1/receive"

	status, body := declareStandard(t, srv, path, map[string]any{"minutes_per_appointment": 45, "minutes_per_asn": 5})
	if status != http.StatusCreated {
		t.Fatalf("first declare = %d %s, want 201", status, body)
	}
	if got := decodeMap(t, body); got["minutes_per_appointment"] != 45.0 || got["minutes_per_asn"] != 5.0 || got["location"] != "SIM1" || got["path_id"] != "receive" {
		t.Fatalf("declared body = %v", got)
	}

	status, body = declareStandard(t, srv, path, map[string]any{"minutes_per_appointment": 60})
	if status != http.StatusOK {
		t.Fatalf("second declare = %d %s, want 200", status, body)
	}
	if got := decodeMap(t, body); got["minutes_per_appointment"] != 60.0 || got["minutes_per_asn"] != 0.0 {
		t.Fatalf("replacement body = %v", got)
	}

	status, _, body = getPath(t, srv, "/inbound-labour-standards?location=SIM1")
	if status != http.StatusOK {
		t.Fatalf("list = %d %s", status, body)
	}
	list := decodeMap(t, body)
	standards, isList := list["standards"].([]any)
	if !isList || len(standards) != 1 || list["location"] != "SIM1" {
		t.Fatalf("list body = %v, want exactly the one replaced standard", list)
	}
}

func TestInboundLabourStandard_InvalidIs422(t *testing.T) {
	srv, _ := newInboundObservedServer(t)
	for name, payload := range map[string]map[string]any{
		"zero appointment minutes": {"minutes_per_appointment": 0},
		"negative asn minutes":     {"minutes_per_appointment": 30, "minutes_per_asn": -1},
	} {
		status, body := declareStandard(t, srv, "/inbound-labour-standards/SIM1/receive", payload)
		if status != http.StatusUnprocessableEntity {
			t.Errorf("%s = %d %s, want 422", name, status, body)
		}
	}
}

// requireFields fails unless every wanted key of got holds the wanted value.
func requireFields(t *testing.T, what string, got, want map[string]any) {
	t.Helper()
	for key, value := range want {
		if got[key] != value {
			t.Errorf("%s: %s = %v, want %v (body %v)", what, key, got[key], value, got)
		}
	}
}

func TestInboundDemand_PricedByStandardAndFailsVisibleWithout(t *testing.T) {
	srv, appointments := newInboundObservedServer(t)
	start := time.Date(2026, 10, 5, 8, 0, 0, 0, time.UTC)
	booked, err := inbound.NewBooked(inbound.BookedParams{
		AppointmentID: "A1", Location: "SIM1", WindowStart: start.Add(time.Hour), WindowEnd: start.Add(2 * time.Hour),
		ASNCount: 2, AsOf: start,
	})
	if err != nil {
		t.Fatalf("NewBooked: %v", err)
	}
	if _, err := appointments.Apply(t.Context(), booked); err != nil {
		t.Fatalf("Apply: %v", err)
	}
	query := "/inbound-demand?location=SIM1&path_id=receive&window_start=2026-10-05T08:00:00Z&window_end=2026-10-05T16:00:00Z"

	status, _, body := getPath(t, srv, query)
	if status != http.StatusOK {
		t.Fatalf("demand = %d %s", status, body)
	}
	requireFields(t, "without a standard the appointment is counted, unpriced and flagged", decodeMap(t, body),
		map[string]any{"appointments": 1.0, "labour_minutes": 0.0, "standard_missing": true, "standard_declared": false})

	if status, body := declareStandard(t, srv, "/inbound-labour-standards/SIM1/receive", map[string]any{"minutes_per_appointment": 40, "minutes_per_asn": 10}); status != http.StatusCreated {
		t.Fatalf("declare = %d %s", status, body)
	}
	_, _, body = getPath(t, srv, query)
	// 40 per appointment + 2 ASNs * 10 = 60 minutes over an 8h window.
	requireFields(t, "priced demand", decodeMap(t, body), map[string]any{
		"labour_minutes": 60.0, "asns": 2.0, "standard_missing": false, "standard_declared": true,
		"as_of": "2026-10-05T08:00:00Z", "source": "inbound-receiving", "average_heads": 60.0 / 480.0,
	})
}

func TestInboundDemand_BadQueryIs400(t *testing.T) {
	srv, _ := newInboundObservedServer(t)
	for name, query := range map[string]string{
		"missing location": "/inbound-demand?path_id=receive&window_start=2026-10-05T08:00:00Z&window_end=2026-10-05T09:00:00Z",
		"missing path":     "/inbound-demand?location=SIM1&window_start=2026-10-05T08:00:00Z&window_end=2026-10-05T09:00:00Z",
		"bad start":        "/inbound-demand?location=SIM1&path_id=receive&window_start=nope&window_end=2026-10-05T09:00:00Z",
		"bad end":          "/inbound-demand?location=SIM1&path_id=receive&window_start=2026-10-05T08:00:00Z&window_end=nope",
	} {
		if status, _, body := getPath(t, srv, query); status != http.StatusBadRequest {
			t.Errorf("%s = %d %s, want 400", name, status, body)
		}
	}
}

func TestInboundDemand_ReversedWindowIs400(t *testing.T) {
	srv, _ := newInboundObservedServer(t)
	status, _, body := getPath(t, srv, "/inbound-demand?location=SIM1&path_id=receive&window_start=2026-10-05T09:00:00Z&window_end=2026-10-05T08:00:00Z")
	if status != http.StatusBadRequest {
		t.Fatalf("reversed window = %d %s, want 400 (invalid-capacity-window, as every other window read)", status, body)
	}
}

func TestPlanVariance_UnknownPlanIs404AndBadFactorIs400(t *testing.T) {
	srv, _ := newInboundObservedServer(t)
	if status, _, body := getPath(t, srv, "/capacity-plans/missing/variance"); status != http.StatusNotFound {
		t.Errorf("unknown plan = %d %s, want 404", status, body)
	}
	if status, _, body := getPath(t, srv, "/capacity-plans/missing/variance?tasks_per_order=abc"); status != http.StatusBadRequest {
		t.Errorf("non-numeric tasks_per_order = %d %s, want 400", status, body)
	}
}

func TestInboundObservedRoutes_AbsentWhenNotWired(t *testing.T) {
	srv := httptest.NewServer(inboundhttp.NewRouter(&inboundhttp.Server{}))
	defer srv.Close()
	for _, p := range []string{"/inbound-labour-standards", "/inbound-demand", "/capacity-plans/x/variance"} {
		if status, _, _ := getPath(t, srv, p); status != http.StatusNotFound && status != http.StatusMethodNotAllowed {
			t.Errorf("%s = %d, want 404/405 when its use case is not wired", p, status)
		}
	}
}
