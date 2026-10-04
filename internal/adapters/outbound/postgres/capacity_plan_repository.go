package postgres

import (
	"context"
	"errors"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/claudioed/warehouse-planning/internal/domain/capacityplan"
	"github.com/claudioed/warehouse-planning/internal/domain/processcapacity"
)

// CapacityPlanRepo is a pgxpool-backed ports.CapacityPlanRepository. Inside
// a ports.UnitOfWork it uses the transaction carried by ctx (pgtx), so its
// Save commits together with the outbox insert.
type CapacityPlanRepo struct {
	pool *pgxpool.Pool
}

// NewCapacityPlanRepo constructs a CapacityPlanRepo over pool.
func NewCapacityPlanRepo(pool *pgxpool.Pool) *CapacityPlanRepo {
	return &CapacityPlanRepo{pool: pool}
}

// Save upserts the plan row. Only state is persisted; pending domain
// events go through the outbox.
func (r *CapacityPlanRepo) Save(ctx context.Context, p *capacityplan.CapacityPlan) error {
	var publishedAt *time.Time
	if !p.PublishedAt().IsZero() {
		t := p.PublishedAt()
		publishedAt = &t
	}
	warnings := p.Warnings()
	if warnings == nil {
		warnings = []string{} // the column is NOT NULL: never encode a nil slice
	}
	_, err := queryFor(ctx, r.pool).Exec(ctx, `
		INSERT INTO capacity_plans (
			id, warehouse_id, location, window_start, window_end, process_path_id,
			assigned_demand, path_capacity, bottleneck_step, capacity_over_window, shortage,
			status, created_at, published_at, bottleneck_constraint, warnings, demand_source)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17)
		ON CONFLICT (id) DO UPDATE SET
			status = EXCLUDED.status,
			published_at = EXCLUDED.published_at
	`, p.ID(), p.WarehouseID(), p.Location(), p.Window().Start(), p.Window().End(), p.ProcessPathID(),
		p.AssignedDemand(), p.PathCapacity(), string(p.BottleneckStep()), p.CapacityOverWindow(), p.Shortage(),
		string(p.Status()), p.CreatedAt(), publishedAt, string(p.BottleneckConstraint()), warnings, string(p.DemandSource()))
	return err
}

// FindByID loads the plan, or (nil, nil) if there is none. Inside a
// UnitOfWork the row is locked (FOR UPDATE) until the transaction ends, so
// two concurrent publishes of the same DRAFT plan serialize and the second
// observes PUBLISHED.
func (r *CapacityPlanRepo) FindByID(ctx context.Context, id string) (*capacityplan.CapacityPlan, error) {
	query := `SELECT ` + planColumns + ` FROM capacity_plans WHERE id = $1`
	q := queryFor(ctx, r.pool)
	if _, inTx := q.(pgx.Tx); inTx {
		query += " FOR UPDATE"
	}

	plan, err := scanPlan(q.QueryRow(ctx, query, id))
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, nil
		}
		return nil, err
	}
	return plan, nil
}

// ListRecent returns up to limit plans, newest first (created_at DESC, id
// DESC so the order is total), optionally restricted to one location. It
// never locks a row: a list is a plain read, in or out of a UnitOfWork.
func (r *CapacityPlanRepo) ListRecent(ctx context.Context, location string, limit int) ([]*capacityplan.CapacityPlan, error) {
	rows, err := queryFor(ctx, r.pool).Query(ctx, `
		SELECT `+planColumns+` FROM capacity_plans
		WHERE ($1::text = '' OR location = $1::text)
		ORDER BY created_at DESC, id DESC
		LIMIT $2`, location, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	out := make([]*capacityplan.CapacityPlan, 0)
	for rows.Next() {
		plan, err := scanPlan(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, plan)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return out, nil
}

// planColumns is the column list scanPlan expects, in order.
const planColumns = `id, warehouse_id, location, window_start, window_end, process_path_id,
		       assigned_demand, path_capacity, bottleneck_step, capacity_over_window, shortage,
		       status, created_at, published_at, bottleneck_constraint, warnings, demand_source`

// scanPlan rehydrates one capacity_plans row selected with planColumns.
func scanPlan(row pgx.Row) (*capacityplan.CapacityPlan, error) {
	var (
		p                      capacityplan.RehydrateParams
		windowStart, windowEnd time.Time
		bottleneck, status     string
		bottleneckConstraint   string
		demandSource           string
		publishedAt            *time.Time
	)
	err := row.Scan(
		&p.ID, &p.WarehouseID, &p.Location, &windowStart, &windowEnd, &p.ProcessPathID,
		&p.AssignedDemand, &p.PathCapacity, &bottleneck, &p.CapacityOverWindow, &p.Shortage,
		&status, &p.CreatedAt, &publishedAt, &bottleneckConstraint, &p.Warnings, &demandSource)
	if err != nil {
		return nil, err
	}

	p.Window, err = processcapacity.NewCapacityWindow(windowStart.UTC(), windowEnd.UTC())
	if err != nil {
		return nil, err
	}
	p.BottleneckStep = processcapacity.ProcessType(bottleneck)
	p.BottleneckConstraint = processcapacity.ConstraintType(bottleneckConstraint)
	p.DemandSource = capacityplan.DemandSource(demandSource)
	p.Status = capacityplan.Status(status)
	p.CreatedAt = p.CreatedAt.UTC()
	if publishedAt != nil {
		p.PublishedAt = publishedAt.UTC()
	}
	return capacityplan.Rehydrate(p), nil
}
