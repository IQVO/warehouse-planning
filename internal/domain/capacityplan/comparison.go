package capacityplan

import (
	"errors"
	"fmt"
	"strings"
)

// The bounds of a comparison (docs/adr/0024 decision 4).
const (
	MinComparedPlans = 2
	MaxComparedPlans = 5
)

var (
	// ErrInvalidComparison is returned when a comparison does not name 2 to
	// 5 distinct, non-blank plan ids.
	ErrInvalidComparison = errors.New("capacityplan: a comparison takes 2 to 5 distinct plan ids")

	// ErrComparisonSiteMismatch is returned when the compared plans are not
	// all of one site.
	ErrComparisonSiteMismatch = errors.New("capacityplan: the compared plans are not all of one site")

	// ErrComparisonWindowMismatch is returned when the compared plans are not
	// all of one planning window (same start and end instants).
	ErrComparisonWindowMismatch = errors.New("capacityplan: the compared plans are not all of one window")
)

// ValidateComparisonIDs reports ErrInvalidComparison unless ids holds
// MinComparedPlans to MaxComparedPlans ids, none blank and none repeated.
func ValidateComparisonIDs(ids []string) error {
	if len(ids) < MinComparedPlans || len(ids) > MaxComparedPlans {
		return fmt.Errorf("%w: got %d", ErrInvalidComparison, len(ids))
	}
	seen := make(map[string]bool, len(ids))
	for _, id := range ids {
		if strings.TrimSpace(id) == "" {
			return fmt.Errorf("%w: a plan id is blank", ErrInvalidComparison)
		}
		if seen[id] {
			return fmt.Errorf("%w: %s is named twice", ErrInvalidComparison, id)
		}
		seen[id] = true
	}
	return nil
}

// CheckComparable reports whether plans can be laid side by side: every plan
// of the first plan's site (ErrComparisonSiteMismatch) and window
// (ErrComparisonWindowMismatch). Their paths, locations and statuses may
// differ: each is one option for the same site and window. It reads the
// stored plans only; nothing is recomputed.
func CheckComparable(plans []*CapacityPlan) error {
	if len(plans) == 0 {
		return ErrInvalidComparison
	}
	first := plans[0]
	for _, p := range plans[1:] {
		if p.siteID != first.siteID {
			return fmt.Errorf("%w: %s is of site %q, %s of site %q", ErrComparisonSiteMismatch, first.id, first.siteID, p.id, p.siteID)
		}
		if !p.window.Start().Equal(first.window.Start()) || !p.window.End().Equal(first.window.End()) {
			return fmt.Errorf("%w: %s and %s cover different windows", ErrComparisonWindowMismatch, first.id, p.id)
		}
	}
	return nil
}
