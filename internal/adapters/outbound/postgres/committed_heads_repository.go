package postgres

import (
	"context"
	"errors"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/claudioed/warehouse-planning/internal/application/ports"
	"github.com/claudioed/warehouse-planning/internal/domain/labourplan"
)

// CommittedHeadsRepo is a pgxpool-backed ports.CommittedHeadsRepository over
// the labour_plan_commitments table (migration 0015, docs/adr/0023 decision
// 3). Inside a ports.UnitOfWork it uses the transaction carried by ctx, so
// the commitment, the drift record and the outbox insert of the event commit
// or roll back together.
type CommittedHeadsRepo struct {
	pool *pgxpool.Pool
}

// NewCommittedHeadsRepo constructs a CommittedHeadsRepo over pool.
func NewCommittedHeadsRepo(pool *pgxpool.Pool) *CommittedHeadsRepo {
	return &CommittedHeadsRepo{pool: pool}
}

// Get implements ports.CommittedHeadsRepository. FOR UPDATE locks the row
// until the enclosing transaction ends, so the commitment trigger and the
// plan trigger read, compare and record one after the other on a key.
func (r *CommittedHeadsRepo) Get(ctx context.Context, siteID, shiftID, pathID string) (labourplan.Commitment, bool, error) {
	c := labourplan.Commitment{SiteID: siteID, ShiftID: shiftID, PathID: pathID}
	var last *int
	err := queryFor(ctx, r.pool).QueryRow(ctx, `
		SELECT committed_heads, observed_at, last_drift_heads FROM labour_plan_commitments
		WHERE site_id = $1 AND shift_id = $2 AND path_id = $3
		FOR UPDATE`, siteID, shiftID, pathID).Scan(&c.Heads, &c.ObservedAt, &last)
	if errors.Is(err, pgx.ErrNoRows) {
		return labourplan.Commitment{}, false, nil
	}
	if err != nil {
		return labourplan.Commitment{}, false, err
	}
	if last != nil {
		c.LastDriftHeads, c.HasLastDrift = *last, true
	}
	return c, true, nil
}

// SaveCommitment implements ports.CommittedHeadsRepository: an upsert of
// committed_heads and observed_at that never touches last_drift_heads.
func (r *CommittedHeadsRepo) SaveCommitment(ctx context.Context, c labourplan.Commitment) error {
	_, err := queryFor(ctx, r.pool).Exec(ctx, `
		INSERT INTO labour_plan_commitments (site_id, shift_id, path_id, committed_heads, observed_at)
		VALUES ($1, $2, $3, $4, $5)
		ON CONFLICT (site_id, shift_id, path_id)
		DO UPDATE SET committed_heads = EXCLUDED.committed_heads, observed_at = EXCLUDED.observed_at`,
		c.SiteID, c.ShiftID, c.PathID, c.Heads, c.ObservedAt)
	return err
}

// RecordDrift implements ports.CommittedHeadsRepository: zero rows updated is
// ports.ErrCommittedHeadsNotFound.
func (r *CommittedHeadsRepo) RecordDrift(ctx context.Context, siteID, shiftID, pathID string, driftHeads int) error {
	tag, err := queryFor(ctx, r.pool).Exec(ctx, `
		UPDATE labour_plan_commitments SET last_drift_heads = $4
		WHERE site_id = $1 AND shift_id = $2 AND path_id = $3`, siteID, shiftID, pathID, driftHeads)
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		return ports.ErrCommittedHeadsNotFound
	}
	return nil
}
