package usecases

import (
	"context"

	"github.com/claudioed/warehouse-planning/internal/application/ports"
	"github.com/claudioed/warehouse-planning/internal/domain/processpath"
)

// ListProcessPaths returns every ProcessPath registered in this context,
// ordered by id. It exists so a client (the console remote) can offer the
// paths a plan or a capacity query can be run against; there is no other way
// to discover a path id.
type ListProcessPaths struct {
	Paths ports.ProcessPathLister
}

// Handle lists the registered paths (an empty, non-nil slice when none is).
func (uc *ListProcessPaths) Handle(ctx context.Context) ([]processpath.ProcessPath, error) {
	paths, err := uc.Paths.List(ctx)
	if err != nil {
		return nil, err
	}
	if paths == nil {
		paths = []processpath.ProcessPath{}
	}
	return paths, nil
}
