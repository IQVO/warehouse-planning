package postgres

import (
	"context"
	"errors"
	"time"

	"github.com/claudioed/warehouse-planning/internal/domain/pagecursor"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/claudioed/warehouse-planning/internal/application/ports"
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

// onePublishedPerKey is migration 0017's partial unique index: one PUBLISHED
// plan per (site_id, location, process_path_id, window_start, window_end).
const onePublishedPerKey = "capacity_plans_one_published_per_key"

// Save upserts the plan row. Only state is persisted; pending domain
// events go through the outbox. A row that would be a second PUBLISHED plan
// of its key violates onePublishedPerKey: that is ports.ErrVersionConflict (a
// concurrent publish of the key committed first), and inside a UnitOfWork the
// transaction is aborted, so the caller reruns the whole unit.
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
	labour, err := encodeLabourRequirement(p.LabourRequirement())
	if err != nil {
		return err
	}
	_, err = queryFor(ctx, r.pool).Exec(ctx, `
		INSERT INTO capacity_plans (
			id, warehouse_id, site_id, location, window_start, window_end, process_path_id,
			assigned_demand, path_capacity, bottleneck_step, capacity_over_window, shortage,
			status, created_at, published_at, bottleneck_constraint, warnings, demand_source,
			inbound_appointments, inbound_labour_minutes, inbound_standard_missing, labour_requirement, forecast_path_id, scenario,
			superseded_by)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19, $20, $21, $22::jsonb, NULLIF($23, ''), NULLIF($24, ''),
			NULLIF($25, ''))
		ON CONFLICT (id) DO UPDATE SET
			status = EXCLUDED.status,
			published_at = EXCLUDED.published_at,
			superseded_by = EXCLUDED.superseded_by
	`, p.ID(), p.WarehouseID(), p.SiteID(), p.Location(), p.Window().Start(), p.Window().End(), p.ProcessPathID(),
		p.AssignedDemand(), p.PathCapacity(), string(p.BottleneckStep()), p.CapacityOverWindow(), p.Shortage(),
		string(p.Status()), p.CreatedAt(), publishedAt, string(p.BottleneckConstraint()), warnings, string(p.DemandSource()),
		p.Inbound().Appointments, p.Inbound().LabourMinutes, p.Inbound().StandardMissing, string(labour), p.ForecastPathID(), p.Scenario(),
		p.SupersededBy())
	if isOnePublishedPerKeyViolation(err) {
		return ports.ErrVersionConflict
	}
	return err
}

// isOnePublishedPerKeyViolation reports a unique violation (SQLSTATE 23505)
// of onePublishedPerKey; any other error, unique or not, is not one.
func isOnePublishedPerKeyViolation(err error) bool {
	var pgErr *pgconn.PgError
	return errors.As(err, &pgErr) && pgErr.Code == "23505" && pgErr.ConstraintName == onePublishedPerKey
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

// FindPublished returns the PUBLISHED plan of key whose id is not excludeID
// (the newest by published_at, then id), or (nil, nil). Inside a UnitOfWork
// the row is locked (FOR UPDATE): a concurrent publish of the same key waits
// for this transaction, then no longer sees the row as PUBLISHED and loses
// on the partial unique index instead (ports.ErrVersionConflict from Save).
func (r *CapacityPlanRepo) FindPublished(ctx context.Context, key capacityplan.SupersedeKey, excludeID string) (*capacityplan.CapacityPlan, error) {
	query := `SELECT ` + planColumns + ` FROM capacity_plans
		WHERE site_id = $1 AND location = $2 AND process_path_id = $3
		  AND window_start = $4 AND window_end = $5
		  AND status = 'PUBLISHED' AND id <> $6
		ORDER BY published_at DESC NULLS LAST, id DESC
		LIMIT 1`
	q := queryFor(ctx, r.pool)
	if _, inTx := q.(pgx.Tx); inTx {
		query += " FOR UPDATE"
	}
	plan, err := scanPlan(q.QueryRow(ctx, query, key.SiteID, key.Location, key.PathID, key.WindowStart, key.WindowEnd, excludeID))
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, nil
		}
		return nil, err
	}
	return plan, nil
}

// ListRecent returns up to limit plans matching filter, newest first
// (created_at DESC, id DESC so the order is total); every filter field that
// is set must match. It never locks a row: a list is a plain read, in or out
// of a UnitOfWork.
func (r *CapacityPlanRepo) ListRecent(ctx context.Context, filter capacityplan.ListFilter, after *pagecursor.Cursor, limit int) ([]*capacityplan.CapacityPlan, error) {
	var afterAt *time.Time
	afterID := ""
	if after != nil {
		at, err := time.Parse(time.RFC3339Nano, after.Sort)
		if err != nil {
			return nil, err
		}
		afterAt, afterID = &at, after.ID
	}
	rows, err := queryFor(ctx, r.pool).Query(ctx, `
		SELECT `+planColumns+` FROM capacity_plans
		WHERE ($1::text = '' OR location = $1::text)
		  AND ($2::text = '' OR site_id = $2::text)
		  AND ($3::text = '' OR process_path_id = $3::text)
		  AND ($4::timestamptz IS NULL OR window_start = $4::timestamptz)
		  AND ($5::timestamptz IS NULL OR window_end = $5::timestamptz)
		  AND ($6::timestamptz IS NULL OR (created_at, id) < ($6::timestamptz, $7::text))
		ORDER BY created_at DESC, id DESC
		LIMIT $8`, filter.Location, filter.SiteID, filter.PathID, filter.WindowStart, filter.WindowEnd, afterAt, afterID, limit)
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
const planColumns = `id, warehouse_id, site_id, location, window_start, window_end, process_path_id,
		       assigned_demand, path_capacity, bottleneck_step, capacity_over_window, shortage,
		       status, created_at, published_at, bottleneck_constraint, warnings, demand_source,
		       inbound_appointments, inbound_labour_minutes, inbound_standard_missing, labour_requirement,
		       COALESCE(forecast_path_id, ''), COALESCE(scenario, ''), COALESCE(superseded_by, '')`

// scanPlan rehydrates one capacity_plans row selected with planColumns.
func scanPlan(row pgx.Row) (*capacityplan.CapacityPlan, error) {
	var (
		p                      capacityplan.RehydrateParams
		windowStart, windowEnd time.Time
		bottleneck, status     string
		bottleneckConstraint   string
		demandSource           string
		publishedAt            *time.Time
		labour                 []byte
	)
	err := row.Scan(
		&p.ID, &p.WarehouseID, &p.SiteID, &p.Location, &windowStart, &windowEnd, &p.ProcessPathID,
		&p.AssignedDemand, &p.PathCapacity, &bottleneck, &p.CapacityOverWindow, &p.Shortage,
		&status, &p.CreatedAt, &publishedAt, &bottleneckConstraint, &p.Warnings, &demandSource,
		&p.Inbound.Appointments, &p.Inbound.LabourMinutes, &p.Inbound.StandardMissing, &labour, &p.ForecastPathID, &p.Scenario,
		&p.SupersededBy)
	if err != nil {
		return nil, err
	}
	if p.LabourRequirement, err = decodeLabourRequirement(labour); err != nil {
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
