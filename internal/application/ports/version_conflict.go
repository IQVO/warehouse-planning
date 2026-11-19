package ports

import "errors"

// ErrVersionConflict is returned by a versioned repository's Save when the
// stored row is not at the version the caller loaded: a first save
// (Version()==1) found a row already there, or a later save found the stored
// version moved past Version()-1. Nothing is written; the caller reloads the
// aggregate, re-applies its change and saves again. WorkloadForecastRepository
// and LabourPlanRepository return it.
var ErrVersionConflict = errors.New("ports: the stored version moved; reload and retry")
