package http

import (
	"errors"
	"net/http"

	"github.com/claudioed/warehouse-planning/internal/application/usecases"
	"github.com/claudioed/warehouse-planning/internal/domain/capacityplan"
	"github.com/claudioed/warehouse-planning/internal/domain/processcapacity"
	"github.com/claudioed/warehouse-planning/internal/domain/processpath"
)

// problemBaseURI is the namespace for this service's RFC 7807 "type" URIs.
// It does not need to resolve to a real page -- it's an identifier, unique
// per distinct error category in this service (matches the fleet's
// convention, e.g. inventory-storage's problemBaseURI).
const problemBaseURI = "https://errors.warehouse-planning.warehouse-systems.dev/"

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
		errors.Is(err, capacityplan.ErrRequiredField):
		return http.StatusUnprocessableEntity
	case errors.Is(err, processcapacity.ErrUnitMismatch),
		errors.Is(err, capacityplan.ErrAlreadyPublished):
		return http.StatusConflict
	case errors.Is(err, usecases.ErrProcessPathNotFound),
		errors.Is(err, usecases.ErrCapacityPlanNotFound):
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
	}
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
