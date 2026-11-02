package usecases

import (
	"context"

	"github.com/claudioed/warehouse-planning/internal/domain/pagecursor"

	"github.com/claudioed/warehouse-planning/internal/application/ports"
	"github.com/claudioed/warehouse-planning/internal/domain/processpath"
)

// ListProcessPaths returns the ProcessPaths registered in this context,
// ordered by id, one keyset page at a time. It exists so a client (the console
// remote) can offer the paths a plan or a capacity query can be run against;
// there is no other way to discover a path id.
type ListProcessPaths struct {
	Paths ports.ProcessPathLister
}

// Handle lists up to limit paths strictly after the cursor (nil = first
// page); an empty, non-nil slice when none follows.
func (uc *ListProcessPaths) Handle(ctx context.Context, after *pagecursor.Cursor, limit int) ([]processpath.ProcessPath, error) {
	paths, err := uc.Paths.ListPage(ctx, after, limit)
	if err != nil {
		return nil, err
	}
	if paths == nil {
		paths = []processpath.ProcessPath{}
	}
	return paths, nil
}
