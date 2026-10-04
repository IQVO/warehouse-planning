// Package ports declares the outbound interfaces the application layer
// depends on. Adapters implement these; the application never imports an
// adapter package (see internal/architecture's hexagonal fitness tests).
package ports

import (
	"context"
	"time"

	"github.com/claudioed/warehouse-planning/internal/domain/processcapacity"
)

// ProcessCapacityRepository persists and retrieves ProcessCapacity
// aggregates, keyed by their identity (ProcessType, Location,
// CapacityWindow).
type ProcessCapacityRepository interface {
	Save(ctx context.Context, pc *processcapacity.ProcessCapacity) error
	// FindByProcessLocationWindow returns the ProcessCapacity registered
	// for EXACTLY the given process + location + window, or (nil, nil) if
	// none has been registered yet. Exact-key semantics: registration and
	// the REST/MCP effective-capacity lookups use it; capacity composition
	// uses FindCovering (docs/adr/0003).
	FindByProcessLocationWindow(ctx context.Context, processType processcapacity.ProcessType, location string, windowStart, windowEnd time.Time) (*processcapacity.ProcessCapacity, error)
	// FindCovering returns every ProcessCapacity of (process, location)
	// whose window COVERS [windowStart, windowEnd) -- window_start <=
	// windowStart AND window_end >= windowEnd, so a window equal to the
	// requested one covers it -- newest first (processcapacity.
	// SortNewestFirst: later window start first, then the narrower window).
	// An empty slice (never an error) when none covers it.
	FindCovering(ctx context.Context, processType processcapacity.ProcessType, location string, windowStart, windowEnd time.Time) ([]*processcapacity.ProcessCapacity, error)
}
