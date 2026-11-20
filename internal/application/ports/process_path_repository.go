package ports

import (
	"context"

	"github.com/claudioed/warehouse-planning/internal/domain/processpath"
)

// ProcessPathRepository persists and retrieves the LOCALLY DECLARED ProcessPath
// read models, keyed by id (RegisterProcessPath, POST /process-paths). The
// process flows consumed from process-path-management (docs/adr/0025) live in
// ProcessFlowRepository and win over a path of the same id at read time.
type ProcessPathRepository interface {
	Save(ctx context.Context, path processpath.ProcessPath) error
	ProcessPathFinder
}

// ProcessPathFinder resolves a path id to its ordered steps. The locally
// declared paths implement it (ProcessPathRepository), and so does the
// flow-first resolver of docs/adr/0025 (usecases.FlowFirstPathFinder), which is
// what the path capacity reads.
type ProcessPathFinder interface {
	// FindByID returns the ProcessPath resolved under id, or (nil, nil) if
	// none is known.
	FindByID(ctx context.Context, id string) (*processpath.ProcessPath, error)
}
