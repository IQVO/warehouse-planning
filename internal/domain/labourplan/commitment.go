package labourplan

import (
	"strings"
	"time"
)

// Commitment is the committed side of the drift (docs/adr/0023 decision 3):
// the heads workforce-management COMMITTED for one (site, shift, path), read
// from its ShiftPlanCommitted, when that commitment was observed, and the
// last drift this context RECORDED for the key. Neither drift trigger ever
// changes the labour plan from it. Build it with NewCommitment, which holds
// the path id in its canonical form, so a commitment for "PICK" meets the
// plan PUT as "pick".
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

// NewCommitment validates one committed line and returns its Commitment,
// the path id in its canonical form (no drift recorded yet). Errors:
// ErrRequiredField for a site, shift or path id blank after trimming, and
// ErrInvalidCount for heads below zero or above MaxCount.
func NewCommitment(siteID, shiftID, pathID string, heads int, observedAt time.Time) (Commitment, error) {
	pathID = CanonicalPathID(pathID)
	if strings.TrimSpace(siteID) == "" || strings.TrimSpace(shiftID) == "" || pathID == "" {
		return Commitment{}, ErrRequiredField
	}
	if heads < 0 || heads > MaxCount {
		return Commitment{}, ErrInvalidCount
	}
	return Commitment{SiteID: siteID, ShiftID: shiftID, PathID: pathID, Heads: heads, ObservedAt: observedAt}, nil
}
