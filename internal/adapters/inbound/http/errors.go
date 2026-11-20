package http

import (
	"errors"
	"net/http"

	"github.com/claudioed/warehouse-planning/internal/application/ports"
	"github.com/claudioed/warehouse-planning/internal/application/usecases"
	"github.com/claudioed/warehouse-planning/internal/domain/capacityplan"
	"github.com/claudioed/warehouse-planning/internal/domain/inbound"
	"github.com/claudioed/warehouse-planning/internal/domain/labourplan"
	"github.com/claudioed/warehouse-planning/internal/domain/observed"
	"github.com/claudioed/warehouse-planning/internal/domain/processcapacity"
	"github.com/claudioed/warehouse-planning/internal/domain/processpath"
	"github.com/claudioed/warehouse-planning/internal/domain/workloadforecast"
)

// problemBaseURI is the namespace for this service's RFC 7807 "type" URIs.
// It does not need to resolve to a real page -- it's an identifier, unique
// per distinct error category in this service (matches the fleet's
// convention, e.g. inventory-storage's problemBaseURI).
const problemBaseURI = "https://errors.warehouse-planning.warehouse-systems.dev/"

// Problem categories shared by several handlers; one definition keeps every
// endpoint's problem+json response identical.
var (
	problemMalformedWindowStart = problemInfo{slug: "malformed-window-start", title: "window_start must be an RFC3339 timestamp"}
	problemMalformedWindowEnd   = problemInfo{slug: "malformed-window-end", title: "window_end must be an RFC3339 timestamp"}
	problemMissingLocation      = problemInfo{slug: "missing-location", title: "location is required"}
)

// problemInfo is the fixed, category-level (type, title) pair for an RFC
// 7807 problem response. slug becomes the last path segment of "type";
// the dynamic detail comes from err.Error() at write time, not this table.
type problemInfo struct {
	slug  string
	title string
}

// statusFor maps a typed domain error to an HTTP status code.
func statusFor(err error) int {
	switch {
	case errors.Is(err, processcapacity.ErrInvalidWindow):
		return http.StatusBadRequest
	case errors.Is(err, processcapacity.ErrNegativeQuantity),
		errors.Is(err, processcapacity.ErrNonPositivePeriod),
		errors.Is(err, processcapacity.ErrNonPositiveStationStandard),
		errors.Is(err, processcapacity.ErrStationStandardRequiredField),
		errors.Is(err, processcapacity.ErrNonPositiveConversionFactor),
		errors.Is(err, processcapacity.ErrMissingConversionFactor),
		errors.Is(err, processcapacity.ErrUnsupportedNormalizationUnit),
		errors.Is(err, processcapacity.ErrMissingStepCapacity),
		errors.Is(err, processpath.ErrEmptySteps),
		errors.Is(err, capacityplan.ErrNegativeDemand),
		errors.Is(err, usecases.ErrMissingAssignedDemand),
		errors.Is(err, capacityplan.ErrRequiredField),
		errors.Is(err, inbound.ErrStandardRequiredField),
		errors.Is(err, inbound.ErrNonPositiveAppointmentMinutes),
		errors.Is(err, inbound.ErrNegativeASNMinutes),
		errors.Is(err, inbound.ErrNonFiniteMinutes),
		errors.Is(err, observed.ErrNonPositiveTasksPerOrder),
		errors.Is(err, capacityplan.ErrNonPositiveTasksPerOrder):
		return http.StatusUnprocessableEntity
	case isRequirementValidation(err), isPlanScenarioValidation(err):
		return http.StatusUnprocessableEntity
	case errors.Is(err, processcapacity.ErrUnitMismatch),
		errors.Is(err, capacityplan.ErrAlreadyPublished),
		errors.Is(err, capacityplan.ErrSuperseded),
		errors.Is(err, ports.ErrVersionConflict):
		return http.StatusConflict
	case errors.Is(err, usecases.ErrProcessPathNotFound),
		errors.Is(err, usecases.ErrCapacityPlanNotFound),
		errors.Is(err, usecases.ErrWorkloadForecastNotFound),
		errors.Is(err, usecases.ErrLabourPlanNotFound):
		return http.StatusNotFound
	default:
		return http.StatusInternalServerError
	}
}

// problemCatalog pins each typed domain error to its RFC 7807
// (type, title) pair. A slice, not a map, because order matters: problemFor
// matches the FIRST entry whose target equals (or wraps) the error.
func problemCatalog() []struct {
	err  error
	info problemInfo
} {
	return []struct {
		err  error
		info problemInfo
	}{
		{processcapacity.ErrInvalidWindow, problemInfo{"invalid-capacity-window", "Capacity window end must be strictly after start"}},
		{processcapacity.ErrNegativeQuantity, problemInfo{"negative-quantity", "Capacity rate quantity must not be negative"}},
		{processcapacity.ErrNonPositivePeriod, problemInfo{"non-positive-period", "Capacity rate period must be positive"}},
		{processcapacity.ErrUnitMismatch, problemInfo{"unit-mismatch", "Constraint rate unit does not match this ProcessCapacity's native unit"}},
		{processcapacity.ErrNonPositiveStationStandard, problemInfo{"non-positive-station-standard", "A station standard's throughput per station must be positive"}},
		{processcapacity.ErrStationStandardRequiredField, problemInfo{"missing-station-standard-field", "A station standard needs a location and a process type"}},
		{processpath.ErrEmptySteps, problemInfo{"empty-process-path-steps", "A ProcessPath must have at least one step"}},
		{processcapacity.ErrNonPositiveConversionFactor, problemInfo{"non-positive-conversion-factor", "A WorkloadProfile conversion factor must be positive"}},
		{processcapacity.ErrMissingConversionFactor, problemInfo{"missing-conversion-factor", "The WorkloadProfile has no conversion factor for one of the path's steps' units"}},
		{processcapacity.ErrUnsupportedNormalizationUnit, problemInfo{"unsupported-normalization-unit", "This step's native unit cannot be normalized to ORDER"}},
		{processcapacity.ErrMissingStepCapacity, problemInfo{"missing-step-capacity", "No registered ProcessCapacity window covers one of the path's steps at this location and window"}},
		{usecases.ErrProcessPathNotFound, problemInfo{"process-path-not-found", "No ProcessPath is registered under this id"}},
		{capacityplan.ErrNegativeDemand, problemInfo{"negative-assigned-demand", "Assigned demand must not be negative"}},
		{usecases.ErrMissingAssignedDemand, problemInfo{"missing-assigned-demand", "assigned_demand is required"}},
		{capacityplan.ErrRequiredField, problemInfo{"missing-required-field", "warehouse_id, location and path_id are required"}},
		{capacityplan.ErrAlreadyPublished, problemInfo{"capacity-plan-already-published", "This CapacityPlan has already been published"}},
		{usecases.ErrCapacityPlanNotFound, problemInfo{"capacity-plan-not-found", "No CapacityPlan exists under this id"}},
		{inbound.ErrStandardRequiredField, problemInfo{"missing-inbound-standard-field", "An inbound labour standard needs a location and a process path id"}},
		{inbound.ErrNonPositiveAppointmentMinutes, problemInfo{"non-positive-appointment-minutes", "Minutes per appointment must be positive"}},
		{inbound.ErrNegativeASNMinutes, problemInfo{"negative-asn-minutes", "Minutes per ASN must not be negative"}},
		{inbound.ErrNonFiniteMinutes, problemInfo{"non-finite-minutes", "Labour minutes must be finite numbers"}},
		{observed.ErrNonPositiveTasksPerOrder, problemInfo{"non-positive-tasks-per-order", "tasks_per_order must be a positive number"}},
		{capacityplan.ErrNonPositiveTasksPerOrder, problemInfo{"non-positive-tasks-per-order", "tasks_per_order must be a positive number"}},

		// Workload forecast and labour plan (docs/adr/0023).
		{workloadforecast.ErrRequiredField, problemInfo{"missing-forecast-field", "site_id and path_id are required"}},
		{workloadforecast.ErrNoBuckets, problemInfo{"no-forecast-buckets", "A workload forecast needs at least one CPT bucket"}},
		{workloadforecast.ErrMissingCPT, problemInfo{"missing-cpt", "Every forecast bucket needs a cpt"}},
		{workloadforecast.ErrNegativeQuantity, problemInfo{"negative-bucket-quantity", "A bucket quantity must be a non-negative whole number of units"}},
		{usecases.ErrWorkloadForecastNotFound, problemInfo{"workload-forecast-not-found", "No workload forecast exists for this site and path"}},
		{labourplan.ErrRequiredField, problemInfo{"missing-labour-plan-field", "site_id, shift_id and path_id are required"}},
		{labourplan.ErrInvalidCount, problemInfo{"invalid-count", "planned_heads and installed_stations must be whole numbers from 0 to 2147483647"}},
		{labourplan.ErrHeadsExceedStations, problemInfo{"heads-exceed-stations", "planned_heads must not exceed installed_stations"}},
		{labourplan.ErrInvalidRate, problemInfo{"non-positive-rate", "rate_units_per_hour must be positive"}},
		{labourplan.ErrInvalidHours, problemInfo{"non-positive-hours", "hours must be positive"}},
		{labourplan.ErrThroughputNotFinite, problemInfo{"throughput-not-finite", "rate_units_per_hour x planned_heads x hours must be a finite number"}},
		{usecases.ErrMissingPlannedHeads, problemInfo{"missing-planned-heads", "planned_heads is required when it cannot be derived from the workload forecast"}},
		{usecases.ErrLabourPlanNotFound, problemInfo{"labour-plan-not-found", "No labour plan exists for this site, shift and path"}},
		{ports.ErrVersionConflict, problemInfo{"version-conflict", "The stored version moved twice during this request; retry it"}},

		// Forecast demand and plan scenarios (docs/adr/0024).
		{capacityplan.ErrUnknownDemandSource, problemInfo{"unknown-demand-source", "demand_source must be request, orders or forecast"}},
		{capacityplan.ErrMissingUnitsPerOrder, problemInfo{"missing-units-per-order", "units_per_order is required with demand_source forecast"}},
		{capacityplan.ErrDemandSourceConflict, problemInfo{"demand-source-conflict", "The request names an input its demand_source does not read"}},
		{capacityplan.ErrInvalidScenario, problemInfo{"invalid-scenario", "scenario must be 1 to 64 characters"}},
		{capacityplan.ErrSuperseded, problemInfo{"capacity-plan-superseded", "This CapacityPlan was superseded by a newer published plan and cannot be published again"}},
		{capacityplan.ErrInvalidComparison, problemInfo{"invalid-comparison-ids", "ids must name 2 to 5 distinct capacity plans"}},
		{capacityplan.ErrComparisonSiteMismatch, problemInfo{"comparison-site-mismatch", "The compared capacity plans are not all of one site"}},
		{capacityplan.ErrComparisonWindowMismatch, problemInfo{"comparison-window-mismatch", "The compared capacity plans are not all of one window"}},
	}
}

// isPlanScenarioValidation reports the 422 validation errors of forecast
// demand, plan scenarios and the comparison read (docs/adr/0024).
func isPlanScenarioValidation(err error) bool {
	for _, target := range []error{
		capacityplan.ErrUnknownDemandSource, capacityplan.ErrMissingUnitsPerOrder, capacityplan.ErrDemandSourceConflict,
		capacityplan.ErrInvalidScenario,
		capacityplan.ErrInvalidComparison, capacityplan.ErrComparisonSiteMismatch, capacityplan.ErrComparisonWindowMismatch,
	} {
		if errors.Is(err, target) {
			return true
		}
	}
	return false
}

// isRequirementValidation reports the 422 validation errors of the workload
// forecast and the labour plan (docs/adr/0023).
func isRequirementValidation(err error) bool {
	for _, target := range []error{
		workloadforecast.ErrRequiredField, workloadforecast.ErrNoBuckets,
		workloadforecast.ErrMissingCPT, workloadforecast.ErrNegativeQuantity,
		labourplan.ErrRequiredField, labourplan.ErrInvalidCount, labourplan.ErrHeadsExceedStations,
		labourplan.ErrInvalidRate, labourplan.ErrInvalidHours, labourplan.ErrThroughputNotFinite,
		usecases.ErrMissingPlannedHeads,
	} {
		if errors.Is(err, target) {
			return true
		}
	}
	return false
}

// problemFor maps a typed domain error to its RFC 7807 (type, title) pair
// by walking problemCatalog in order. Mirrors statusFor's groupings.
func problemFor(err error) problemInfo {
	for _, entry := range problemCatalog() {
		if errors.Is(err, entry.err) {
			return entry.info
		}
	}
	return problemInfo{"internal-error", "An unexpected internal error occurred"}
}
