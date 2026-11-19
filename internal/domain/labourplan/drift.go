package labourplan

import "time"

// Drift compares the labour plan's heads with the heads workforce-management
// committed for the same (site, shift, path) (docs/adr/0023 decision 3, the
// surfacing rule of wes-work-planning ADR 0019 kept).
type Drift struct {
	PlannedHeads   int
	CommittedHeads int
	// Heads is CommittedHeads - PlannedHeads: positive when workforce
	// committed more heads than the plan requires.
	Heads int
}

// CompareHeads returns the drift between the plan and the commitment.
func CompareHeads(plannedHeads, committedHeads int) Drift {
	return Drift{PlannedHeads: plannedHeads, CommittedHeads: committedHeads, Heads: committedHeads - plannedHeads}
}

// Detected reports a non-zero drift.
func (d Drift) Detected() bool { return d.Heads != 0 }

// ShouldRaise reports whether this drift is news: it is non-zero and differs
// from the last drift recorded for the key (hasLast=false when none was
// recorded). Callers record every computed drift, zero included, so a drift
// that clears and then returns is raised again, while a repeat of the same
// drift (a redelivered or unchanged commitment) is not.
func (d Drift) ShouldRaise(last int, hasLast bool) bool {
	return d.Detected() && (!hasLast || d.Heads != last)
}

// Event is the LabourPlanDriftDetected event for this drift.
func (d Drift) Event(siteID, shiftID, pathID string, at time.Time) DriftDetected {
	return DriftDetected{
		SiteID:         siteID,
		ShiftID:        shiftID,
		PathID:         pathID,
		PlannedHeads:   d.PlannedHeads,
		CommittedHeads: d.CommittedHeads,
		DriftHeads:     d.Heads,
		At:             at,
	}
}
