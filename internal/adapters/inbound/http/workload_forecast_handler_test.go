package http_test

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	inboundhttp "github.com/claudioed/warehouse-planning/internal/adapters/inbound/http"
	outboundkafka "github.com/claudioed/warehouse-planning/internal/adapters/outbound/kafka"
	"github.com/claudioed/warehouse-planning/internal/adapters/outbound/memory"
	"github.com/claudioed/warehouse-planning/internal/application/ports"
	"github.com/claudioed/warehouse-planning/internal/application/usecases"
	"github.com/claudioed/warehouse-planning/internal/domain/workloadforecast"
)

const forecastPath = "/workload-forecasts/SIM1/PICK"

// alwaysConflictingForecasts loses every save race.
type alwaysConflictingForecasts struct{ *memory.WorkloadForecastRepo }

func (alwaysConflictingForecasts) Save(context.Context, *workloadforecast.Forecast) error {
	return ports.ErrVersionConflict
}

type forecastServer struct {
	*httptest.Server
	outbox *memory.OutboxRepo
}

func newForecastServer(t *testing.T, forecasts ports.WorkloadForecastRepository) *forecastServer {
	t.Helper()
	ob := memory.NewOutboxRepo()
	srv := httptest.NewServer(inboundhttp.NewRouter(&inboundhttp.Server{
		ReceiveWorkloadForecast: &usecases.ReceiveWorkloadForecast{
			Forecasts: forecasts, Outbox: ob, Encoder: outboundkafka.NewFanoutEncoder(), UnitOfWork: memory.NewUnitOfWork(ob),
		},
		GetWorkloadForecast: &usecases.GetWorkloadForecast{Forecasts: forecasts},
	}))
	t.Cleanup(srv.Close)
	return &forecastServer{Server: srv, outbox: ob}
}

type forecastBody struct {
	SiteID        string `json:"site_id"`
	PathID        string `json:"path_id"`
	TotalQuantity int64  `json:"total_quantity"`
	ReceivedAt    string `json:"received_at"`
	Version       int64  `json:"version"`
	Buckets       []struct {
		CPT      string `json:"cpt"`
		Quantity int64  `json:"quantity"`
	} `json:"buckets"`
}

func decodeForecast(t *testing.T, body []byte) forecastBody {
	t.Helper()
	var f forecastBody
	if err := json.Unmarshal(body, &f); err != nil {
		t.Fatalf("decode forecast: %v (%s)", err, body)
	}
	return f
}

func buckets(pairs ...any) map[string]any {
	out := make([]map[string]any, 0, len(pairs)/2)
	for i := 0; i+1 < len(pairs); i += 2 {
		out = append(out, map[string]any{"cpt": pairs[i], "quantity": pairs[i+1]})
	}
	return map[string]any{"buckets": out}
}

// expectForecast checks one forecast answer and the outbox size after it.
func (s *forecastServer) expectForecast(t *testing.T, step string, status int, body []byte, wantStatus int, wantVersion, wantTotal int64, wantOutbox int) forecastBody {
	t.Helper()
	got := decodeForecast(t, body)
	if status != wantStatus || got.Version != wantVersion || got.TotalQuantity != wantTotal {
		t.Fatalf("%s = %d %+v, want %d version %d total %d", step, status, got, wantStatus, wantVersion, wantTotal)
	}
	if n := len(s.outbox.Messages()); n != wantOutbox {
		t.Fatalf("%s: outbox = %d rows, want %d", step, n, wantOutbox)
	}
	return got
}

func TestHandler_WorkloadForecast_CreateReplaceAndIdenticalReceipt(t *testing.T) {
	srv := newForecastServer(t, memory.NewWorkloadForecastRepo())

	status, hdr, body := putJSON(t, srv.Server, forecastPath, buckets("2026-11-19T10:00:00Z", 120, "2026-11-19T11:00:00-03:00", 80))
	if hdr.Get("Content-Type") != "application/json" {
		t.Fatalf("first PUT content type = %s", hdr.Get("Content-Type"))
	}
	// The fan-out encoder writes the integration message and its analytics copy.
	first := srv.expectForecast(t, "first PUT", status, body, http.StatusCreated, 1, 200, 2)
	if first.SiteID != "SIM1" || first.PathID != "PICK" || len(first.Buckets) != 2 || first.Buckets[1].CPT != "2026-11-19T14:00:00Z" || first.ReceivedAt == "" {
		t.Fatalf("first = %+v", first)
	}

	status, _, body = putJSON(t, srv.Server, forecastPath, buckets("2026-11-19T10:00:00Z", 120, "2026-11-19T14:00:00Z", 80))
	srv.expectForecast(t, "identical PUT", status, body, http.StatusOK, 1, 200, 2)

	status, _, body = putJSON(t, srv.Server, forecastPath, buckets("2026-11-19T10:00:00Z", 30))
	srv.expectForecast(t, "replacing PUT", status, body, http.StatusOK, 2, 30, 4)

	status, _, body = getPath(t, srv.Server, forecastPath)
	srv.expectForecast(t, "GET", status, body, http.StatusOK, 2, 30, 4)
}

func TestHandler_WorkloadForecast_Problems(t *testing.T) {
	cases := map[string]struct {
		body   any
		status int
		slug   string
	}{
		"no buckets":        {map[string]any{"buckets": []any{}}, http.StatusUnprocessableEntity, "no-forecast-buckets"},
		"buckets omitted":   {map[string]any{}, http.StatusUnprocessableEntity, "no-forecast-buckets"},
		"negative quantity": {buckets("2026-11-19T10:00:00Z", -1), http.StatusUnprocessableEntity, "negative-bucket-quantity"},
		"missing cpt":       {map[string]any{"buckets": []any{map[string]any{"quantity": 3}}}, http.StatusUnprocessableEntity, "missing-cpt"},
		"missing quantity":  {map[string]any{"buckets": []any{map[string]any{"cpt": "2026-11-19T10:00:00Z"}}}, http.StatusUnprocessableEntity, "missing-bucket-quantity"},
		"malformed cpt":     {buckets("tomorrow", 3), http.StatusBadRequest, "malformed-cpt"},
		"fractional units":  {buckets("2026-11-19T10:00:00Z", 1.5), http.StatusBadRequest, "malformed-json"},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			srv := newForecastServer(t, memory.NewWorkloadForecastRepo())
			status, hdr, body := putJSON(t, srv.Server, forecastPath, tc.body)
			if status != tc.status || hdr.Get("Content-Type") != "application/problem+json" || problemType(t, body) != tc.slug {
				t.Fatalf("PUT = %d %s %s, want %d %s", status, hdr.Get("Content-Type"), body, tc.status, tc.slug)
			}
			if n := len(srv.outbox.Messages()); n != 0 {
				t.Fatalf("outbox = %d, want 0", n)
			}
		})
	}
}

func TestHandler_WorkloadForecast_UnknownForecastIs404(t *testing.T) {
	srv := newForecastServer(t, memory.NewWorkloadForecastRepo())
	status, _, body := getPath(t, srv.Server, forecastPath)
	if status != http.StatusNotFound || problemType(t, body) != "workload-forecast-not-found" {
		t.Fatalf("GET = %d %s, want 404 workload-forecast-not-found", status, body)
	}
}

func TestHandler_WorkloadForecast_ARepeatedVersionConflictIs409(t *testing.T) {
	srv := newForecastServer(t, alwaysConflictingForecasts{memory.NewWorkloadForecastRepo()})
	status, _, body := putJSON(t, srv.Server, forecastPath, buckets("2026-11-19T10:00:00Z", 1))
	if status != http.StatusConflict || problemType(t, body) != "version-conflict" {
		t.Fatalf("PUT = %d %s, want 409 version-conflict", status, body)
	}
}

func TestHandler_WorkloadForecast_RoutesAbsentWhenNotWired(t *testing.T) {
	srv := httptest.NewServer(inboundhttp.NewRouter(&inboundhttp.Server{}))
	t.Cleanup(srv.Close)
	if status, _, _ := getPath(t, srv, forecastPath); status != http.StatusNotFound {
		t.Fatalf("GET without the use case = %d, want the router's 404", status)
	}
}
