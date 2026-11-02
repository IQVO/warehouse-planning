package ports

import (
	"context"

	"github.com/claudioed/warehouse-planning/internal/domain/pagecursor"

	"github.com/claudioed/warehouse-planning/internal/domain/processcapacity"
)

// StationStandardRepository persists the operator-declared StationStandards,
// keyed by (location, process type). Declaring an existing key replaces the
// throughput wholesale.
type StationStandardRepository interface {
	// Save upserts the standard under its (location, process type) key.
	Save(ctx context.Context, standard processcapacity.StationStandard) error
	// Find returns the standard declared for location + processType, or
	// (nil, nil) if none has been declared.
	Find(ctx context.Context, location string, processType processcapacity.ProcessType) (*processcapacity.StationStandard, error)
	// List returns the standards declared for location ordered by process
	// type, or every standard (ordered by location, then process type) when
	// location is empty.
	List(ctx context.Context, location string) ([]processcapacity.StationStandard, error)
	// ListPage is List as a keyset page: at most limit standards ordered by
	// (location, process type), strictly after the cursor (Sort = location,
	// ID = process type) when after is not nil.
	ListPage(ctx context.Context, location string, after *pagecursor.Cursor, limit int) ([]processcapacity.StationStandard, error)
}
