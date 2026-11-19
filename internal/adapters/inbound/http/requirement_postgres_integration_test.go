//go:build integration

// End-to-end over REAL Postgres for the requirement side of docs/adr/0023:
// the real router, the Postgres forecast, labour-plan, tally and outbox
// repositories, the Postgres UnitOfWork and the fan-out encoder cmd/api wires
// (wireRequirementSide). It proves the planner's morning: PUT the workload
// forecast, PUT the labour plan WITHOUT heads (derived from that forecast),
// and both CloudEvents types land in outbox_events on the integration and the
// analytics topic in the same transaction as their rows. The database is the
// package's shared testcontainers Postgres (TestMain in
// postgres_lifecycle_integration_test.go), one private clone per test.
package http_test

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"

	inboundhttp "github.com/claudioed/warehouse-planning/internal/adapters/inbound/http"
	outboundkafka "github.com/claudioed/warehouse-planning/internal/adapters/outbound/kafka"
	"github.com/claudioed/warehouse-planning/internal/adapters/outbound/postgres"
	"github.com/claudioed/warehouse-planning/internal/application/usecases"
)

const (
	typeForecastReceived = "com.warehouse.wes.warehouse-planning.workloadforecast.WorkloadForecastReceived"
	typePlanCommitted    = "com.warehouse.wes.warehouse-planning.labourplan.LabourPlanCommitted"
)

// newRequirementRouter wires the ADR 0023 routes over one private migrated
// database, the way cmd/api does, and returns the server and the pool.
func newRequirementRouter(t *testing.T) (*httptest.Server, *pgxpool.Pool) {
	t.Helper()
	pool, err := postgres.NewPool(context.Background(), httpMigratedDB(t))
	if err != nil {
		t.Fatalf("open pool: %v", err)
	}
	t.Cleanup(pool.Close)
	forecasts, plans := postgres.NewWorkloadForecastRepo(pool), postgres.NewLabourPlanRepo(pool)
	ob, tally, uow := postgres.NewOutboxRepo(pool), postgres.NewStorageTallyRepo(pool), postgres.NewUnitOfWork(pool)
	encoder := outboundkafka.NewFanoutEncoder()
	srv := httptest.NewServer(inboundhttp.NewRouter(&inboundhttp.Server{
		ReceiveWorkloadForecast: &usecases.ReceiveWorkloadForecast{Forecasts: forecasts, Outbox: ob, Encoder: encoder, UnitOfWork: uow},
		GetWorkloadForecast:     &usecases.GetWorkloadForecast{Forecasts: forecasts},
		CommitLabourPlan: &usecases.CommitLabourPlan{
			Plans: plans, Forecasts: forecasts, Outbox: ob, Encoder: encoder, UnitOfWork: uow, Stations: tally,
		},
		GetLabourPlan:   &usecases.GetLabourPlan{Plans: plans, Stations: tally},
		ListLabourPlans: &usecases.ListLabourPlans{Plans: plans, Stations: tally},
	}))
	t.Cleanup(srv.Close)
	return srv, pool
}

func countOutbox(t *testing.T, pool *pgxpool.Pool, eventType string) int {
	t.Helper()
	var n int
	if err := pool.QueryRow(context.Background(), `SELECT count(*) FROM outbox_events WHERE event_type = $1`, eventType).Scan(&n); err != nil {
		t.Fatalf("count outbox: %v", err)
	}
	return n
}

func TestPostgres_ForecastThenLabourPlanWithoutHeads_OutboxCarriesBothTypes(t *testing.T) {
	srv, pool := newRequirementRouter(t)

	status, _, body := putJSON(t, srv, "/workload-forecasts/SIM1/PICK", buckets("2026-11-19T07:00:00Z", 500, "2026-11-19T12:00:00Z", 300))
	if status != http.StatusCreated {
		t.Fatalf("PUT forecast = %d %s", status, body)
	}
	status, _, body = putJSON(t, srv, "/labour-plans/SIM1/S1/PICK", map[string]any{
		"rate_units_per_hour": 10, "hours": 8, "installed_stations": 12, "shift_start": "2026-11-19T06:00:00Z",
	})
	plan := decodeLabourPlan(t, body)
	if status != http.StatusCreated || plan.PlannedHeads != 10 || plan.HeadsSource != "forecast" || plan.Version != 1 {
		t.Fatalf("PUT plan = %d %s, want 201 with 10 heads from the forecast", status, body)
	}

	// One row per stream (integration + analytics) for each type.
	if got := countOutbox(t, pool, typeForecastReceived); got != 2 {
		t.Fatalf("%s rows = %d, want 2", typeForecastReceived, got)
	}
	if got := countOutbox(t, pool, typePlanCommitted); got != 2 {
		t.Fatalf("%s rows = %d, want 2", typePlanCommitted, got)
	}

	// The identical PUTs write nothing and keep the versions.
	if status, _, body = putJSON(t, srv, "/labour-plans/SIM1/S1/PICK", map[string]any{
		"rate_units_per_hour": 10, "hours": 8, "installed_stations": 12, "shift_start": "2026-11-19T06:00:00Z",
	}); status != http.StatusOK || decodeLabourPlan(t, body).Version != 1 || countOutbox(t, pool, typePlanCommitted) != 2 {
		t.Fatalf("identical PUT plan = %d %s", status, body)
	}
	if status, _, body = putJSON(t, srv, "/workload-forecasts/SIM1/PICK", buckets("2026-11-19T07:00:00Z", 500, "2026-11-19T12:00:00Z", 300)); status != http.StatusOK || countOutbox(t, pool, typeForecastReceived) != 2 {
		t.Fatalf("identical PUT forecast = %d %s", status, body)
	}

	if status, _, body = getPath(t, srv, "/labour-plans?site_id=SIM1&shift_id=S1"); status != http.StatusOK {
		t.Fatalf("list = %d %s", status, body)
	}
}
