package capacityplan

import (
	"errors"
	"math"

	"github.com/claudioed/warehouse-planning/internal/domain/processcapacity"
)

// DemandSourceForecast: assigned_demand was derived from the WorkloadForecast
// of (site id, forecast path id): the units of the buckets whose CPT falls in
// the plan window, over units_per_order, rounded up (docs/adr/0024 decision 1).
const DemandSourceForecast DemandSource = "forecast"

var (
	// ErrUnknownDemandSource is returned for a requested demand source that
	// is not request, orders or forecast.
	ErrUnknownDemandSource = errors.New("capacityplan: demand_source must be request, orders or forecast")

	// ErrMissingUnitsPerOrder is returned when the demand comes from the
	// forecast and units_per_order is absent: forecast volume is in UNITS and
	// a plan's demand is in ORDERS; they are never compared without the
	// WorkloadProfile factor.
	ErrMissingUnitsPerOrder = errors.New("capacityplan: units_per_order is required with demand_source forecast")

	// ErrDemandSourceConflict is returned when a request names an input its
	// demand source never reads (a stated assigned_demand with a derived
	// source, a forecast_path_id without the forecast source): it is refused,
	// never silently ignored.
	ErrDemandSourceConflict = errors.New("capacityplan: the request names an input its demand_source does not read")
)

// ParseDemandSource validates a requested demand source. The empty string is
// "not named" and is returned as is: the caller applies the default (request
// when assigned_demand is stated, orders when it is omitted).
func ParseDemandSource(raw string) (DemandSource, error) {
	switch s := DemandSource(raw); s {
	case "", DemandSourceRequest, DemandSourceOrders, DemandSourceForecast:
		return s, nil
	default:
		return "", ErrUnknownDemandSource
	}
}

// ValidateForecastFactor reports whether units_per_order can turn forecast
// units into orders: ErrMissingUnitsPerOrder when it is absent and
// processcapacity.ErrNonPositiveConversionFactor when it is not positive.
func ValidateForecastFactor(unitsPerOrder *float64) error {
	if unitsPerOrder == nil {
		return ErrMissingUnitsPerOrder
	}
	if *unitsPerOrder <= 0 {
		return processcapacity.ErrNonPositiveConversionFactor
	}
	return nil
}

// ForecastOrders is the demand, in ORDERS, of units forecast units at
// unitsPerOrder (a factor ValidateForecastFactor accepted): the quotient
// rounded UP, so a part order is a whole order to serve. The quotient is
// shrunk by one part in 10^12 first, so a division whose exact result is
// whole (7 units at 0.7 is 10.000000000000002 in float64) is not rounded up
// to the next order.
func ForecastOrders(units int64, unitsPerOrder float64) float64 {
	return math.Ceil(float64(units) / unitsPerOrder * (1 - wholeTolerance))
}

// wholeTolerance is the relative binary-rounding error ForecastOrders absorbs.
const wholeTolerance = 1e-12

// forecastPathFor keeps the forecast path id only on a plan whose demand came
// from the forecast: any other plan read no forecast.
func forecastPathFor(source DemandSource, pathID string) string {
	if source != DemandSourceForecast {
		return ""
	}
	return pathID
}
