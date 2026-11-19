package usecases

import (
	"context"
	"errors"
	"time"

	"github.com/claudioed/warehouse-planning/internal/application/ports"
	"github.com/claudioed/warehouse-planning/internal/domain/workloadforecast"
)

// ErrWorkloadForecastNotFound is returned when no WorkloadForecast exists for
// the requested (site id, path id).
var ErrWorkloadForecastNotFound = errors.New("usecases: no workload forecast exists for this site and path")

// ReceiveWorkloadForecastCommand is one receipt of the volume a process path
// is expected to receive at a site, as CPT buckets (docs/adr/0023 decision 1).
// The buckets replace the stored ones as a whole.
type ReceiveWorkloadForecastCommand struct {
	SiteID  string
	PathID  string
	Buckets []workloadforecast.Bucket
}

// ReceiveWorkloadForecastResult is the forecast after the receipt. Created is
// true for a first receipt (version 1); Changed is true whenever the receipt
// was stored (a first receipt or different buckets). An identical receipt is
// Created=false, Changed=false: nothing was written and no event was queued.
type ReceiveWorkloadForecastResult struct {
	Forecast *workloadforecast.Forecast
	Created  bool
	Changed  bool
}

// ReceiveWorkloadForecast stores a receipt of the WorkloadForecast aggregate:
// it loads the forecast of (site, path), builds it with workloadforecast.New
// when absent or applies Receive otherwise and, when that changed something,
// saves it and inserts its WorkloadForecastReceived CloudEvents into the
// outbox -- the load, the save and the outbox insert are ONE
// ports.UnitOfWork.Do, so they commit or roll back together. A
// ports.ErrVersionConflict (a concurrent receipt moved the stored version
// between the load and the save) re-runs the whole unit once; a second one is
// returned as is.
type ReceiveWorkloadForecast struct {
	Forecasts  ports.WorkloadForecastRepository
	Outbox     ports.OutboxRepository
	Encoder    ports.EventEncoder
	UnitOfWork ports.UnitOfWork

	// Now supplies the receipt time (utcNow when nil).
	Now func() time.Time
}

// Handle applies the receipt. Errors: the aggregate's validation errors
// (workloadforecast.ErrRequiredField, ErrNoBuckets, ErrMissingCPT,
// ErrNegativeQuantity), ports.ErrVersionConflict after the one retry, and any
// store, encoder or outbox failure.
func (uc *ReceiveWorkloadForecast) Handle(ctx context.Context, cmd ReceiveWorkloadForecastCommand) (ReceiveWorkloadForecastResult, error) {
	now := uc.Now
	if now == nil {
		now = utcNow
	}
	var res ReceiveWorkloadForecastResult
	err := retryOnVersionConflict(func() error {
		var err error
		res, err = uc.receive(ctx, cmd, now())
		return err
	})
	if err != nil {
		return ReceiveWorkloadForecastResult{}, err
	}
	return res, nil
}

// receive is one attempt: the whole load-decide-save in one unit of work.
func (uc *ReceiveWorkloadForecast) receive(ctx context.Context, cmd ReceiveWorkloadForecastCommand, at time.Time) (ReceiveWorkloadForecastResult, error) {
	var res ReceiveWorkloadForecastResult
	err := uc.UnitOfWork.Do(ctx, func(ctx context.Context) error {
		stored, found, err := uc.Forecasts.Get(ctx, cmd.SiteID, cmd.PathID)
		if err != nil {
			return err
		}
		var event workloadforecast.Received
		if found {
			res.Forecast = stored
			event, res.Changed, err = stored.Receive(cmd.SiteID, cmd.PathID, cmd.Buckets, at)
		} else {
			res.Forecast, event, err = workloadforecast.New(cmd.SiteID, cmd.PathID, cmd.Buckets, at)
			res.Created, res.Changed = true, true
		}
		if err != nil || !res.Changed {
			return err
		}
		if err := uc.Forecasts.Save(ctx, res.Forecast); err != nil {
			return err
		}
		msgs, err := uc.Encoder.EncodeWorkloadForecastEvents(event)
		if err != nil {
			return err
		}
		return uc.Outbox.Insert(ctx, msgs...)
	})
	return res, err
}

// GetWorkloadForecast reads the stored forecast of (site id, path id). It is
// a pure read: it neither mutates the forecast nor queues an event.
type GetWorkloadForecast struct {
	Forecasts ports.WorkloadForecastRepository
}

// Handle returns the forecast, or ErrWorkloadForecastNotFound.
func (uc *GetWorkloadForecast) Handle(ctx context.Context, siteID, pathID string) (*workloadforecast.Forecast, error) {
	f, found, err := uc.Forecasts.Get(ctx, siteID, pathID)
	if err != nil {
		return nil, err
	}
	if !found {
		return nil, ErrWorkloadForecastNotFound
	}
	return f, nil
}

// retryOnVersionConflict runs attempt and, when it failed with
// ports.ErrVersionConflict (another writer moved the stored version between
// the load and the save), runs it once more from the load. A second conflict
// is returned unchanged, for the caller to report (REST: 409).
func retryOnVersionConflict(attempt func() error) error {
	err := attempt()
	if errors.Is(err, ports.ErrVersionConflict) {
		err = attempt()
	}
	return err
}
