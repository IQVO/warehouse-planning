package ports

import (
	"context"
	"errors"

	"github.com/claudioed/warehouse-planning/internal/domain/labourplan"
)

// ErrCommittedHeadsNotFound is returned by CommittedHeadsRepository.RecordDrift
// when no commitment is stored for the key: a drift is only ever recorded
// against a commitment.
var ErrCommittedHeadsNotFound = errors.New("ports: no committed heads are stored for this site, shift and path")

// CommittedHeadsRepository persists labourplan.Commitment, the committed side
// of the labour plan drift (docs/adr/0023 decision 3): one per (site id,
// shift id, path id). Inside a UnitOfWork it joins the transaction carried in
// ctx, so the commitment, the drift record and the outbox insert of a
// LabourPlanDriftDetected commit or roll back together.
type CommittedHeadsRepository interface {
	// Get returns (labourplan.Commitment{}, false, nil) when no commitment
	// is stored. Inside a UnitOfWork the Postgres adapter locks the row
	// until the unit ends, so the two drift triggers serialize on the key.
	Get(ctx context.Context, siteID, shiftID, pathID string) (labourplan.Commitment, bool, error)
	// SaveCommitment upserts Heads and ObservedAt of c's key. The last
	// recorded drift is kept (none on a first commitment); c's
	// LastDriftHeads and HasLastDrift are ignored.
	SaveCommitment(ctx context.Context, c labourplan.Commitment) error
	// RecordDrift stores driftHeads as the last recorded drift of the key;
	// ErrCommittedHeadsNotFound when no commitment is stored for it.
	RecordDrift(ctx context.Context, siteID, shiftID, pathID string, driftHeads int) error
}
