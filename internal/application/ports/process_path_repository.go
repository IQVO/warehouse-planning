package ports

import (
	"context"

	"github.com/claudioed/warehouse-planning/internal/domain/processpath"
)

// ProcessPathFinder resolves a path id to its ordered steps. Since ADR 0025
// stage 3 dropped the local declaration, the flow resolver
// (usecases.FlowPathFinder) is the only implementation and the path capacity
// reads it.
type ProcessPathFinder interface {
	// FindByID returns the ProcessPath resolved under id, or (nil, nil) if
	// none is known.
	FindByID(ctx context.Context, id string) (*processpath.ProcessPath, error)
}
