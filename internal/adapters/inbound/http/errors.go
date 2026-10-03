package http

import (
	"errors"
	"net/http"

	"github.com/claudioed/warehouse-planning/internal/domain/processcapacity"
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
		errors.Is(err, processcapacity.ErrNonPositivePeriod):
		return http.StatusUnprocessableEntity
	case errors.Is(err, processcapacity.ErrUnitMismatch):
		return http.StatusConflict
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
