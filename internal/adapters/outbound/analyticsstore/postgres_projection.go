package analyticsstore

import (
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/claudioed/warehouse-planning/internal/analytics/report"
)

// Projection is the Postgres WRITER (report.Projection). Apply claims the
// event id and upserts the plan row in ONE transaction: the id is recorded
// if and only if its effect is, so a failure anywhere leaves nothing behind
// and the redelivered event is applied afresh, while a replay of an applied
// id is a no-op.
type Projection struct {
	pool *pgxpool.Pool
}

// NewProjection constructs a Projection over the writer pool.
func NewProjection(pool *pgxpool.Pool) *Projection { return &Projection{pool: pool} }

var _ report.Projection = (*Projection)(nil)

const claimSQL = `
	INSERT INTO analytics_processed_events (event_id, event_type, occurred_at)
	VALUES ($1, $2, $3)
	ON CONFLICT (event_id) DO NOTHING`

// upsertSQL writes only what the event carries: a NULL parameter keeps the
// stored value (COALESCE), so the row converges in any arrival order.
const upsertSQL = `
	INSERT INTO plan_facts (plan_id, warehouse_id, location, bottleneck_step, binding_constraint, shortage, created_at, published_at)
	VALUES ($1, $2, $3, COALESCE($4::text, ''), COALESCE($5::text, ''), COALESCE($6::double precision, 0), $7::timestamptz, $8::timestamptz)
	ON CONFLICT (plan_id) DO UPDATE SET
		warehouse_id       = EXCLUDED.warehouse_id,
		location           = EXCLUDED.location,
		bottleneck_step    = COALESCE($4::text, plan_facts.bottleneck_step),
		binding_constraint = COALESCE($5::text, plan_facts.binding_constraint),
		shortage           = COALESCE($6::double precision, plan_facts.shortage),
		created_at         = COALESCE($7::timestamptz, plan_facts.created_at),
		published_at       = COALESCE($8::timestamptz, plan_facts.published_at)`

// Apply implements report.Projection.
func (p *Projection) Apply(ctx context.Context, e report.PlanEvent) (bool, error) {
	tx, err := p.pool.Begin(ctx)
	if err != nil {
		return false, fmt.Errorf("analyticsstore: begin: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }() // no-op once committed

	tag, err := tx.Exec(ctx, claimSQL, e.EventID, string(e.Kind), e.At)
	if err != nil {
		return false, classify(fmt.Errorf("analyticsstore: claim event %s: %w", e.EventID, err))
	}
	if tag.RowsAffected() == 0 {
		return false, nil // already applied: the deferred rollback ends the empty tx
	}
	if _, err := tx.Exec(ctx, upsertSQL, e.PlanID, e.WarehouseID, e.Location,
		e.BottleneckStep, e.BindingConstraint, e.Shortage, e.CreatedAt, e.PublishedAt); err != nil {
		return false, classify(fmt.Errorf("analyticsstore: project %s %s: %w", e.Kind, e.PlanID, err))
	}
	if err := tx.Commit(ctx); err != nil {
		return false, fmt.Errorf("analyticsstore: commit: %w", err)
	}
	return true, nil
}

// classify wraps a Postgres data-exception (SQLSTATE class 22) or
// integrity-violation (class 23) error in report.ErrRejected: the same
// event can never succeed, so the consumer dead-letters it instead of
// retrying forever. Everything else (connection loss, timeouts, locks,
// failovers) stays transient.
func classify(err error) error {
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) && (pgErr.Code[:2] == "22" || pgErr.Code[:2] == "23") {
		return fmt.Errorf("%w: %w", report.ErrRejected, err)
	}
	return err
}
