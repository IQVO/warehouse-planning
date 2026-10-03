package ports

import (
	"context"

	"github.com/claudioed/warehouse-planning/internal/domain/processpath"
)

// ProcessPathRepository persists and retrieves ProcessPath read models,
// keyed by id. There is no event-driven sync from process-path-management
// yet (a later phase) -- for now a ProcessPath is seeded directly via the
// RegisterProcessPath use case.
type ProcessPathRepository interface {
	Save(ctx context.Context, path processpath.ProcessPath) error
	// FindByID returns the ProcessPath registered under id, or (nil, nil)
	// if none has been registered yet.
	FindByID(ctx context.Context, id string) (*processpath.ProcessPath, error)
}
