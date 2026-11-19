package ports

import (
	"context"

	"github.com/claudioed/warehouse-planning/internal/domain/workloadforecast"
)

// WorkloadForecastRepository persists the WorkloadForecast aggregate
// (docs/adr/0023): one forecast per (site id, path id), its CPT buckets
// stored whole. Inside a UnitOfWork the repository joins the transaction
// carried in ctx, so a Save commits or rolls back with the outbox insert of
// its WorkloadForecastReceived event. It never persists domain events.
type WorkloadForecastRepository interface {
	// Get returns (nil, false, nil) when no forecast exists for (siteID, pathID).
	Get(ctx context.Context, siteID, pathID string) (*workloadforecast.Forecast, bool, error)
	// Save writes the forecast at f.Version(): an INSERT when Version()==1 (a duplicate key
	// is ErrVersionConflict), else an UPDATE guarded by stored version == Version()-1
	// (zero rows is ErrVersionConflict). Buckets are stored whole.
	Save(ctx context.Context, f *workloadforecast.Forecast) error
}
