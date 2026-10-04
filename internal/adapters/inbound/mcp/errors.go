package mcp

import (
	"errors"
	"fmt"
	"log/slog"

	"github.com/claudioed/warehouse-planning/internal/application/usecases"
	"github.com/claudioed/warehouse-planning/internal/domain/capacityplan"
	"github.com/claudioed/warehouse-planning/internal/domain/processcapacity"
	"github.com/claudioed/warehouse-planning/internal/domain/processpath"
)

// slugFor names each typed domain error with the same stable slug the REST
// adapter uses as the last segment of its RFC 7807 "type" URI
// (internal/adapters/inbound/http/errors.go), so a client sees the same
// error vocabulary over both surfaces. ok is false for an untyped error.
func slugFor(err error) (slug string, ok bool) {
	catalog := []struct {
		target error
		slug   string
	}{
		{processcapacity.ErrInvalidWindow, "invalid-capacity-window"},
		{processcapacity.ErrNegativeQuantity, "negative-quantity"},
		{processcapacity.ErrNonPositivePeriod, "non-positive-period"},
		{processcapacity.ErrUnitMismatch, "unit-mismatch"},
		{processcapacity.ErrNonPositiveStationStandard, "non-positive-station-standard"},
		{processcapacity.ErrStationStandardRequiredField, "missing-station-standard-field"},
		{processpath.ErrEmptySteps, "empty-process-path-steps"},
		{processcapacity.ErrNonPositiveConversionFactor, "non-positive-conversion-factor"},
		{processcapacity.ErrMissingConversionFactor, "missing-conversion-factor"},
		{processcapacity.ErrUnsupportedNormalizationUnit, "unsupported-normalization-unit"},
		{processcapacity.ErrMissingStepCapacity, "missing-step-capacity"},
		{usecases.ErrProcessPathNotFound, "process-path-not-found"},
		{capacityplan.ErrNegativeDemand, "negative-assigned-demand"},
		{usecases.ErrMissingAssignedDemand, "missing-assigned-demand"},
		{capacityplan.ErrRequiredField, "missing-required-field"},
		{capacityplan.ErrAlreadyPublished, "capacity-plan-already-published"},
		{usecases.ErrCapacityPlanNotFound, "capacity-plan-not-found"},
	}
	for _, entry := range catalog {
		if errors.Is(err, entry.target) {
			return entry.slug, true
		}
	}
	return "", false
}

// toolError builds a tool-level error "<slug>: <detail>". Returned from a
// handler it becomes an isError tool result, never a transport failure.
func toolError(slug, detail string) error {
	return fmt.Errorf("%s: %s", slug, detail)
}

// mapError turns a use-case/repository error into the tool error the model
// sees. Typed domain errors are prefixed with their REST slug and keep their
// message; anything else is logged and reported generically so infrastructure
// details (DSNs, SQL) never reach the model.
func mapError(err error) error {
	if slug, ok := slugFor(err); ok {
		return toolError(slug, err.Error())
	}
	slog.Error("mcp tool failed with an unexpected error", "error", err)
	return toolError("internal-error", "an unexpected internal error occurred")
}
