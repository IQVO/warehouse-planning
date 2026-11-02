package ports

import (
	"context"

	"github.com/claudioed/warehouse-planning/internal/domain/pagecursor"

	"github.com/claudioed/warehouse-planning/internal/domain/processpath"
)

// ProcessPathLister is the read-only list side of the ProcessPath store. It
// is a separate port from ProcessPathRepository so listing does not widen the
// interface the write and capacity use cases depend on; the memory and
// Postgres repositories implement both.
type ProcessPathLister interface {
	// ListPage returns at most limit ProcessPaths ordered by id, strictly
	// after the cursor's ID when after is not nil (an empty, non-nil slice
	// when nothing follows).
	ListPage(ctx context.Context, after *pagecursor.Cursor, limit int) ([]processpath.ProcessPath, error)
}
