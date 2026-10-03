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
	// for the given process + location + window, or (nil, nil) if none
	// has been registered yet.
	FindByProcessLocationWindow(ctx context.Context, processType processcapacity.ProcessType, location string, windowStart, windowEnd time.Time) (*processcapacity.ProcessCapacity, error)
}
