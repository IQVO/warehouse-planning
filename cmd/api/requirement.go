package main

import (
	inboundhttp "github.com/claudioed/warehouse-planning/internal/adapters/inbound/http"
	"github.com/claudioed/warehouse-planning/internal/application/ports"
	"github.com/claudioed/warehouse-planning/internal/application/usecases"
)

// wireRequirementSide adds the docs/adr/0023 use cases to the REST server:
// the workload forecast and the shift labour plan, both on the Postgres
// repositories, the shared outbox, the fan-out encoder (integration +
// analytics) and the same unit of work as every other write. The labour plan
// reads the forecast to derive omitted heads and the facility station tally
// (ADR 0002) for its station warning. REST only: no MCP tool and no A2A skill
// (ADR 0023 decision 7). Split from buildServer only to keep it within the
// length budget.
func wireRequirementSide(s *inboundhttp.Server, ad adapters, encoder ports.EventEncoder) {
	s.ReceiveWorkloadForecast = &usecases.ReceiveWorkloadForecast{
		Forecasts: ad.workloadForecasts, Outbox: ad.outbox, Encoder: encoder, UnitOfWork: ad.uow,
	}
	s.GetWorkloadForecast = &usecases.GetWorkloadForecast{Forecasts: ad.workloadForecasts}
	s.CommitLabourPlan = &usecases.CommitLabourPlan{
		Plans: ad.labourPlans, Forecasts: ad.workloadForecasts,
		Outbox: ad.outbox, Encoder: encoder, UnitOfWork: ad.uow, Stations: ad.tallyReader,
		// ADR 0023 decision 3: a changed plan surfaces its drift against
		// the heads workforce-management committed for the key.
		Commitments: ad.committedHeads,
	}
	s.GetLabourPlan = &usecases.GetLabourPlan{Plans: ad.labourPlans, Stations: ad.tallyReader}
	s.ListLabourPlans = &usecases.ListLabourPlans{Plans: ad.labourPlans, Stations: ad.tallyReader}
}
