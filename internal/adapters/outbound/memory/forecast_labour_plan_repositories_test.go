package memory_test

import (
	"testing"

	"github.com/claudioed/warehouse-planning/internal/adapters/outbound/memory"
	"github.com/claudioed/warehouse-planning/internal/adapters/outbound/repocontract"
	"github.com/claudioed/warehouse-planning/internal/application/ports"
)

var (
	_ ports.WorkloadForecastRepository = (*memory.WorkloadForecastRepo)(nil)
	_ ports.LabourPlanRepository       = (*memory.LabourPlanRepo)(nil)
)

// The in-memory doubles show the behaviour the Postgres adapters show
// (postgres/forecast_labour_plan_integration_test.go runs the same suite).
func TestForecastAndLabourPlanDoublesSatisfyTheRepositoryContract(t *testing.T) {
	repocontract.Run(t, func(*testing.T) repocontract.Fixture {
		forecasts, plans := memory.NewWorkloadForecastRepo(), memory.NewLabourPlanRepo()
		return repocontract.Fixture{UoW: memory.NewUnitOfWork(forecasts, plans), Forecasts: forecasts, Plans: plans}
	})
}
