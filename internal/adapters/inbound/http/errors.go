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

// replayClass is how a first answer relates to a retry under the same
// Idempotency-Key (fleet rule, docs/adr/0026): FINAL answers are stored and
// replayed, OUTCOME answers are stored AND their writes commit with the
// stored answer, RETRYABLE answers are not stored at all (the key is
// released and a retry executes again). Declared per problem type on its
// catalogue entry; an entry without one fails the replay-class tests.
type replayClass string

const (
	replayFinal     replayClass = "FINAL"
	replayOutcome   replayClass = "OUTCOME"
	replayRetryable replayClass = "RETRYABLE"
)

// Problem categories shared by several handlers; one definition keeps every
// endpoint's problem+json response identical. Every problemInfo literal of
// this package lives in THIS file (errors_catalogue_test.go enforces it), so
// the class a handler answers with is always the catalogue's.
var (
	problemMalformedWindowStart = problemInfo{slug: "malformed-window-start", title: "window_start must be an RFC3339 timestamp", class: replayFinal}
	problemMalformedWindowEnd   = problemInfo{slug: "malformed-window-end", title: "window_end must be an RFC3339 timestamp", class: replayFinal}
	problemMissingLocation      = problemInfo{slug: "missing-location", title: "location is required", class: replayFinal}

	problemMalformedJSON             = problemInfo{slug: "malformed-json", title: "Request body is not valid JSON", class: replayFinal}
	problemNonPositivePeriod         = problemInfo{slug: "non-positive-period", title: "period_seconds must be positive", class: replayFinal}
	problemProcessCapacityNotFound   = problemInfo{slug: "process-capacity-not-found", title: "No ProcessCapacity is registered for this process, location and window", class: replayRetryable}
	problemUnknownQueryParameter     = problemInfo{slug: "unknown-query-parameter", title: "Unknown query parameter", class: replayFinal}
	problemInvalidLimit              = problemInfo{slug: "invalid-limit", title: "limit must be an integer between 1 and 500", class: replayFinal}
	problemInvalidCursor             = problemInfo{slug: "invalid-cursor", title: "cursor is not a valid page token", class: replayFinal}
	problemMissingPathID             = problemInfo{slug: "missing-path-id", title: "path_id is required", class: replayFinal}
	problemMalformedTasksPerOrder    = problemInfo{slug: "malformed-tasks-per-order", title: "tasks_per_order must be a number", class: replayFinal}
	problemMalformedUnitsPerOrder    = problemInfo{slug: "malformed-units-per-order", title: "units_per_order must be a number", class: replayFinal}
	problemMalformedPackagesPerOrder = problemInfo{slug: "malformed-packages-per-order", title: "packages_per_order must be a number", class: replayFinal}
	problemInvalidReportRange        = problemInfo{slug: "invalid-report-range", title: "from and to must form a valid RFC 3339 range of at most 366 days", class: replayFinal}
	problemReportStoreError          = problemInfo{slug: "report-store-error", title: "The report could not be served", class: replayRetryable}
	problemDependencyUnavailable     = problemInfo{slug: "dependency-unavailable", title: "A runtime dependency is unavailable", class: replayRetryable}
	problemResourceNotFound          = problemInfo{slug: "resource-not-found", title: "Resource not found", class: replayFinal}
	problemMalformedRequestBody      = problemInfo{slug: "malformed-request-body", title: "Request body could not be read", class: replayFinal}
	problemMalformedRequestQuery     = problemInfo{slug: "malformed-request-query", title: "Query string must not contain a NUL character", class: replayFinal}
	problemNULRequestBody            = problemInfo{slug: "malformed-request-body", title: "Request body must not contain a NUL character", class: replayFinal}
	problemInternalError             = problemInfo{slug: "internal-error", title: "An unexpected internal error occurred", class: replayRetryable}
	problemIdempotencyKeyRequired    = problemInfo{slug: "idempotency-key-required", title: "The Idempotency-Key header is required", class: replayFinal}
	problemIdempotencyKeyInvalid     = problemInfo{slug: "idempotency-key-invalid", title: "The Idempotency-Key header is malformed", class: replayFinal}
	problemIdempotencyKeyReused      = problemInfo{slug: "idempotency-key-reused", title: "Idempotency-Key was already used with a different request", class: replayFinal}

	problemMalformedShiftStart      = problemInfo{slug: "malformed-shift-start", title: "shift_start must be an RFC3339 timestamp", class: replayFinal}
	problemMissingInstalledStations = problemInfo{slug: "missing-installed-stations", title: "installed_stations is required", class: replayFinal}
	problemMissingSiteID            = problemInfo{slug: "missing-site-id", title: "site_id is required", class: replayFinal}
	problemMissingShiftID           = problemInfo{slug: "missing-shift-id", title: "shift_id is required", class: replayFinal}
	problemMalformedCPT             = problemInfo{slug: "malformed-cpt", title: "cpt must be an RFC3339 timestamp", class: replayFinal}
	problemMissingBucketQuantity    = problemInfo{slug: "missing-bucket-quantity", title: "Every forecast bucket needs a quantity", class: replayFinal}
)

// problemInfo is the fixed, category-level (type, title, replay class) pair
// for an RFC 7807 problem response. slug becomes the last path segment of
// "type"; the dynamic detail comes from err.Error() at write time, not this
// table. class says what a retry under the same Idempotency-Key gets
// (docs/adr/0026); writeProblem marks the idempotency middleware's recorder
// with it, and the middleware holds no list of slugs.
type problemInfo struct {
	slug  string
	title string
	class replayClass
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
// (type, title, replay class) triple. A slice, not a map, because order
// matters: problemFor matches the FIRST entry whose target equals (or wraps)
// the error. The class of each entry is decided in docs/adr/0026.
func problemCatalog() []struct {
	err  error
	info problemInfo
} {
	return []struct {
		err  error
		info problemInfo
	}{
		// FINAL: a function of the request bytes alone.
		{processcapacity.ErrInvalidWindow, problemInfo{"invalid-capacity-window", "Capacity window end must be strictly after start", replayFinal}},
		{processcapacity.ErrNegativeQuantity, problemInfo{"negative-quantity", "Capacity rate quantity must not be negative", replayFinal}},
		{processcapacity.ErrNonPositivePeriod, problemInfo{"non-positive-period", "Capacity rate period must be positive", replayFinal}},
		// FINAL: the aggregate's native unit is one-way -- set by the first
		// registration and never changed (AddConstraint refuses a mismatch and
		// no aggregate is ever deleted), so the identical request is refused
		// for the aggregate's whole life (docs/adr/0026).
		{processcapacity.ErrUnitMismatch, problemInfo{"unit-mismatch", "Constraint rate unit does not match this ProcessCapacity's native unit", replayFinal}},
		{processcapacity.ErrNonPositiveStationStandard, problemInfo{"non-positive-station-standard", "A station standard's throughput per station must be positive", replayFinal}},
		{processcapacity.ErrStationStandardRequiredField, problemInfo{"missing-station-standard-field", "A station standard needs a location and a process type", replayFinal}},
		// RETRYABLE: the ACTIVE flow's stages are event-fed state (ADR 0025);
		// a newer flow version can carry the steps.
		{processpath.ErrEmptySteps, problemInfo{"empty-process-path-steps", "A ProcessPath must have at least one step", replayRetryable}},
		{processcapacity.ErrNonPositiveConversionFactor, problemInfo{"non-positive-conversion-factor", "A WorkloadProfile conversion factor must be positive", replayFinal}},
		// RETRYABLE: WHICH factor is missing depends on the units of the
		// ACTIVE flow's steps and the covering capacities (registered by other
		// requests); a mixed request-only/state cause (docs/adr/0026).
		{processcapacity.ErrMissingConversionFactor, problemInfo{"missing-conversion-factor", "The WorkloadProfile has no conversion factor for one of the path's steps' units", replayRetryable}},
		// RETRYABLE: the offending unit belongs to a registered constraint or
		// a declared station standard -- state other requests can change.
		{processcapacity.ErrUnsupportedNormalizationUnit, problemInfo{"unsupported-normalization-unit", "This step's native unit cannot be normalized to ORDER", replayRetryable}},
		// RETRYABLE: a covering ProcessCapacity window can be registered later.
		{processcapacity.ErrMissingStepCapacity, problemInfo{"missing-step-capacity", "No registered ProcessCapacity window covers one of the path's steps at this location and window", replayRetryable}},
		// RETRYABLE: the flow arrives as a process-path-management event (or a
		// local declaration) a moment later.
		{usecases.ErrProcessPathNotFound, problemInfo{"process-path-not-found", "No ProcessPath is registered under this id", replayRetryable}},
		{capacityplan.ErrNegativeDemand, problemInfo{"negative-assigned-demand", "Assigned demand must not be negative", replayFinal}},
		// RETRYABLE: assigned_demand omitted and the expected-demand read model
		// holds no orders (yet) -- order events land later (ADR 0004).
		{usecases.ErrMissingAssignedDemand, problemInfo{"missing-assigned-demand", "assigned_demand is required", replayRetryable}},
		{capacityplan.ErrRequiredField, problemInfo{"missing-required-field", "warehouse_id, location and path_id are required", replayFinal}},
		// FINAL: PUBLISHED is one-way -- nothing unpublishes, and supersede
		// only moves PUBLISHED to SUPERSEDED (docs/adr/0026).
		{capacityplan.ErrAlreadyPublished, problemInfo{"capacity-plan-already-published", "This CapacityPlan has already been published", replayFinal}},
		// FINAL: plan ids are minted by this service (uuid, CreateCapacityPlan)
		// and rows are never deleted, so an unknown id can never appear later.
		{usecases.ErrCapacityPlanNotFound, problemInfo{"capacity-plan-not-found", "No CapacityPlan exists under this id", replayFinal}},
		{inbound.ErrStandardRequiredField, problemInfo{"missing-inbound-standard-field", "An inbound labour standard needs a location and a process path id", replayFinal}},
		{inbound.ErrNonPositiveAppointmentMinutes, problemInfo{"non-positive-appointment-minutes", "Minutes per appointment must be positive", replayFinal}},
		{inbound.ErrNegativeASNMinutes, problemInfo{"negative-asn-minutes", "Minutes per ASN must not be negative", replayFinal}},
		{inbound.ErrNonFiniteMinutes, problemInfo{"non-finite-minutes", "Labour minutes must be finite numbers", replayFinal}},
		{observed.ErrNonPositiveTasksPerOrder, problemInfo{"non-positive-tasks-per-order", "tasks_per_order must be a positive number", replayFinal}},
		{capacityplan.ErrNonPositiveTasksPerOrder, problemInfo{"non-positive-tasks-per-order", "tasks_per_order must be a positive number", replayFinal}},

		// Workload forecast and labour plan (docs/adr/0023).
		{workloadforecast.ErrRequiredField, problemInfo{"missing-forecast-field", "site_id and path_id are required", replayFinal}},
		{workloadforecast.ErrNoBuckets, problemInfo{"no-forecast-buckets", "A workload forecast needs at least one CPT bucket", replayFinal}},
		{workloadforecast.ErrMissingCPT, problemInfo{"missing-cpt", "Every forecast bucket needs a cpt", replayFinal}},
		{workloadforecast.ErrNegativeQuantity, problemInfo{"negative-bucket-quantity", "A bucket quantity must be a non-negative whole number of units", replayFinal}},
		// RETRYABLE: the forecast is a natural-key fact (site, path) another
		// request can PUT later.
		{usecases.ErrWorkloadForecastNotFound, problemInfo{"workload-forecast-not-found", "No workload forecast exists for this site and path", replayRetryable}},
		{labourplan.ErrRequiredField, problemInfo{"missing-labour-plan-field", "site_id, shift_id and path_id are required", replayFinal}},
		{labourplan.ErrInvalidCount, problemInfo{"invalid-count", "planned_heads and installed_stations must be whole numbers from 0 to 2147483647", replayFinal}},
		{labourplan.ErrHeadsExceedStations, problemInfo{"heads-exceed-stations", "planned_heads must not exceed installed_stations", replayFinal}},
		{labourplan.ErrInvalidRate, problemInfo{"non-positive-rate", "rate_units_per_hour must be positive", replayFinal}},
		{labourplan.ErrInvalidHours, problemInfo{"non-positive-hours", "hours must be positive", replayFinal}},
		{labourplan.ErrThroughputNotFinite, problemInfo{"throughput-not-finite", "rate_units_per_hour x planned_heads x hours must be a finite number", replayFinal}},
		// RETRYABLE: planned_heads becomes derivable once a workload forecast
		// for the site and path exists (another request PUTs it).
		{usecases.ErrMissingPlannedHeads, problemInfo{"missing-planned-heads", "planned_heads is required when it cannot be derived from the workload forecast", replayRetryable}},
		// RETRYABLE: the labour plan is a natural-key fact (site, shift, path).
		{usecases.ErrLabourPlanNotFound, problemInfo{"labour-plan-not-found", "No labour plan exists for this site, shift and path", replayRetryable}},
		// RETRYABLE: the optimistic-lock loss -- the canonical retryable.
		{ports.ErrVersionConflict, problemInfo{"version-conflict", "The stored version moved twice during this request; retry it", replayRetryable}},

		// Forecast demand and plan scenarios (docs/adr/0024).
		{capacityplan.ErrUnknownDemandSource, problemInfo{"unknown-demand-source", "demand_source must be request, orders or forecast", replayFinal}},
		{capacityplan.ErrMissingUnitsPerOrder, problemInfo{"missing-units-per-order", "units_per_order is required with demand_source forecast", replayFinal}},
		{capacityplan.ErrDemandSourceConflict, problemInfo{"demand-source-conflict", "The request names an input its demand_source does not read", replayFinal}},
		{capacityplan.ErrInvalidScenario, problemInfo{"invalid-scenario", "scenario must be 1 to 64 characters", replayFinal}},
		// FINAL: SUPERSEDED is terminal -- nothing un-supersedes a plan.
		{capacityplan.ErrSuperseded, problemInfo{"capacity-plan-superseded", "This CapacityPlan was superseded by a newer published plan and cannot be published again", replayFinal}},
		{capacityplan.ErrInvalidComparison, problemInfo{"invalid-comparison-ids", "ids must name 2 to 5 distinct capacity plans", replayFinal}},
		// FINAL: a stored plan's site is immutable after creation (Save only
		// ever changes status, published_at and superseded_by).
		{capacityplan.ErrComparisonSiteMismatch, problemInfo{"comparison-site-mismatch", "The compared capacity plans are not all of one site", replayFinal}},
		{capacityplan.ErrComparisonWindowMismatch, problemInfo{"comparison-window-mismatch", "The compared capacity plans are not all of one window", replayFinal}},
	}
}

// standaloneProblem is one problem answered with a fixed status, written by
// the middleware or a handler directly rather than mapped from a domain
// error. Every such problemInfo of this package is registered here (an AST
// test forbids literals outside this file), so each carries its replay class.
type standaloneProblem struct {
	status int
	info   problemInfo
}

// standaloneProblems lists every problem the service answers without mapping
// a domain error, with the status its writer uses and the replay class
// decided in docs/adr/0026. The 5xx entries carry RETRYABLE per the fleet
// rule (a 5xx is never stored, so the class is RETRYABLE by definition).
func standaloneProblems() []standaloneProblem {
	return []standaloneProblem{
		{http.StatusBadRequest, problemIdempotencyKeyRequired},
		{http.StatusBadRequest, problemIdempotencyKeyInvalid},
		{http.StatusConflict, problemIdempotencyKeyReused},
		{http.StatusBadRequest, problemMalformedRequestBody},
		{http.StatusInternalServerError, problemInternalError},

		{http.StatusBadRequest, problemMalformedJSON},
		{http.StatusBadRequest, problemMalformedWindowStart},
		{http.StatusBadRequest, problemMalformedWindowEnd},
		{http.StatusBadRequest, problemMissingLocation},
		{http.StatusUnprocessableEntity, problemNonPositivePeriod},
		{http.StatusNotFound, problemProcessCapacityNotFound},
		{http.StatusBadRequest, problemUnknownQueryParameter},
		{http.StatusBadRequest, problemInvalidLimit},
		{http.StatusBadRequest, problemInvalidCursor},
		{http.StatusBadRequest, problemMissingPathID},
		{http.StatusBadRequest, problemMalformedTasksPerOrder},
		{http.StatusBadRequest, problemMalformedUnitsPerOrder},
		{http.StatusBadRequest, problemMalformedPackagesPerOrder},
		{http.StatusBadRequest, problemInvalidReportRange},
		{http.StatusInternalServerError, problemReportStoreError},
		{http.StatusServiceUnavailable, problemDependencyUnavailable},
		{http.StatusNotFound, problemResourceNotFound},
		{http.StatusBadRequest, problemMalformedRequestQuery},
		{http.StatusBadRequest, problemNULRequestBody},

		{http.StatusBadRequest, problemMalformedShiftStart},
		{http.StatusBadRequest, problemMissingSiteID},
		{http.StatusBadRequest, problemMissingShiftID},
		{http.StatusBadRequest, problemMalformedCPT},
		{http.StatusUnprocessableEntity, problemMissingBucketQuantity},
		{http.StatusUnprocessableEntity, problemMissingInstalledStations},
	}
}

// problemType is one problem type with the status its writer answers it
// with and its declared replay class.
type problemType struct {
	slug   string
	status int
	class  replayClass
}

// allProblemTypes returns every problem type this service can answer on any
// route, deduplicated by (slug, status): the domain-error catalogue first,
// then the standalone problems. This is the accessor the replay-class tests
// and docs/adr/0026's table are held equal to.
func allProblemTypes() []problemType {
	seen := make(map[[2]string]bool)
	out := make([]problemType, 0, 80)
	add := func(slug string, status int, class replayClass) {
		k := [2]string{slug, http.StatusText(status)}
		if seen[k] {
			return
		}
		seen[k] = true
		out = append(out, problemType{slug: slug, status: status, class: class})
	}
	for _, entry := range problemCatalog() {
		add(entry.info.slug, statusFor(entry.err), entry.info.class)
	}
	for _, p := range standaloneProblems() {
		add(p.info.slug, p.status, p.info.class)
	}
	return out
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
	return problemInternalError
}
