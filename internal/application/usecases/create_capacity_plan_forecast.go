package usecases

import (
	"context"
	"fmt"
	"strings"

	"github.com/claudioed/warehouse-planning/internal/domain/capacityplan"
	"github.com/claudioed/warehouse-planning/internal/domain/workloadforecast"
)

// requestedSource settles the demand source of a create command
// (docs/adr/0024 decision 1). An unnamed source keeps the pre-ADR default:
// request when assigned_demand is stated, orders when it is omitted. A named
// source must agree with the inputs: request needs a stated assigned_demand
// (ErrMissingAssignedDemand otherwise), orders and forecast derive it and so
// refuse a stated one, and forecast_path_id is read by the forecast source
// only (capacityplan.ErrDemandSourceConflict) -- an input is never silently
// ignored.
func requestedSource(cmd CreateCapacityPlanCommand) (capacityplan.DemandSource, error) {
	source, err := capacityplan.ParseDemandSource(cmd.DemandSource)
	if err != nil {
		return "", err
	}
	stated := !cmd.DemandFromOrders
	switch {
	case source == "" && stated:
		source = capacityplan.DemandSourceRequest
	case source == "":
		source = capacityplan.DemandSourceOrders
	case source == capacityplan.DemandSourceRequest && !stated:
		return "", ErrMissingAssignedDemand
	case source != capacityplan.DemandSourceRequest && stated:
		return "", fmt.Errorf("%w: assigned_demand is stated but demand_source %s derives it; omit one of them", capacityplan.ErrDemandSourceConflict, source)
	}
	if strings.TrimSpace(cmd.ForecastPathID) != "" && source != capacityplan.DemandSourceForecast {
		return "", fmt.Errorf("%w: forecast_path_id is read only with demand_source forecast, not %s", capacityplan.ErrDemandSourceConflict, source)
	}
	return source, nil
}

// forecastPathOf is the canonical path code of the forecast a command reads:
// its ForecastPathID, or its ProcessPathID when that is blank.
func forecastPathOf(cmd CreateCapacityPlanCommand) string {
	if path := workloadforecast.CanonicalPathID(cmd.ForecastPathID); path != "" {
		return path
	}
	return workloadforecast.CanonicalPathID(cmd.ProcessPathID)
}

// forecastDemand is the demand of a demand_source forecast: the units of the
// WorkloadForecast of (SiteID, forecastPathOf) whose bucket CPT is in
// [WindowStart, WindowEnd), over units_per_order, rounded up
// (capacityplan.ForecastOrders). units_per_order is checked first, so a
// missing factor is reported whatever the forecast holds. No forecast, no
// bucket in the window or no forecast store wired is ErrMissingAssignedDemand:
// no data is never zero demand.
func (uc *CreateCapacityPlan) forecastDemand(ctx context.Context, cmd CreateCapacityPlanCommand) (capacityplan.LabourDemand, error) {
	if err := capacityplan.ValidateForecastFactor(cmd.UnitsPerOrder); err != nil {
		return capacityplan.LabourDemand{}, err
	}
	if uc.Forecasts == nil {
		return capacityplan.LabourDemand{}, fmt.Errorf("%w: no workload forecast store is wired", ErrMissingAssignedDemand)
	}
	pathID := forecastPathOf(cmd)
	forecast, found, err := uc.Forecasts.Get(ctx, cmd.SiteID, pathID)
	if err != nil {
		return capacityplan.LabourDemand{}, err
	}
	if !found {
		return capacityplan.LabourDemand{}, fmt.Errorf("%w: no workload forecast exists for site %s and path %s", ErrMissingAssignedDemand, cmd.SiteID, pathID)
	}
	units, buckets := forecast.QuantityDueIn(cmd.WindowStart, cmd.WindowEnd)
	if buckets == 0 {
		return capacityplan.LabourDemand{}, fmt.Errorf("%w: no bucket of the workload forecast for site %s and path %s has its cpt in the window", ErrMissingAssignedDemand, cmd.SiteID, pathID)
	}
	return capacityplan.LabourDemand{
		AssignedDemand: capacityplan.ForecastOrders(units, *cmd.UnitsPerOrder),
		Source:         capacityplan.DemandSourceForecast,
	}, nil
}
