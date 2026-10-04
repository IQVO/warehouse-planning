package ports

import (
	"context"

	"github.com/claudioed/warehouse-planning/internal/domain/processpath"
)

// ProcessPathLister is the read-only list side of the ProcessPath store. It
// is a separate port from ProcessPathRepository so listing does not widen the
// interface the write and capacity use cases depend on; the memory and
// Postgres repositories implement both.
type ProcessPathLister interface {
	// List returns every registered ProcessPath ordered by id (an empty,
	// non-nil slice when none is registered).
	List(ctx context.Context) ([]processpath.ProcessPath, error)
}
