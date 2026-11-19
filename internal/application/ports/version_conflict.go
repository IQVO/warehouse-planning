package ports

import "errors"

// ErrVersionConflict is returned by a versioned repository's Save when the
// stored row is not at the version the caller loaded: a first save
// (Version()==1) found a row already there, or a later save found the stored
// version moved past Version()-1. Nothing is written; the caller reloads the
// aggregate, re-applies its change and saves again. WorkloadForecastRepository
// and LabourPlanRepository return it; CapacityPlanRepository returns it when a
// concurrent publish of the same key won (docs/adr/0024 decision 3).
var ErrVersionConflict = errors.New("ports: the stored version moved; reload and retry")
