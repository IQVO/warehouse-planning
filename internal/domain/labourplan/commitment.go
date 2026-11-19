package labourplan

import "time"

// Commitment is the committed side of the drift (docs/adr/0023 decision 3):
// the heads workforce-management COMMITTED for one (site, shift, path), read
// from its ShiftPlanCommitted, when that commitment was observed, and the
// last drift this context RECORDED for the key. Neither drift trigger ever
// changes the labour plan from it.
type Commitment struct {
	SiteID     string
	ShiftID    string
	PathID     string
	Heads      int
	ObservedAt time.Time

	// LastDriftHeads is the last drift recorded for the key (Drift.Heads,
	// zero included). HasLastDrift is false until one was recorded, i.e.
	// while no labour plan existed for the key.
	LastDriftHeads int
	HasLastDrift   bool
}
