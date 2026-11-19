package postgres

import (
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/claudioed/warehouse-planning/internal/domain/labourplan"
)

// LabourPlanRepo is a pgxpool-backed ports.LabourPlanRepository over the
// labour_plans table (migration 0014, docs/adr/0023). Inside a
// ports.UnitOfWork it uses the transaction carried by ctx, so its Save
// commits or rolls back with the outbox insert of the event. Neither Get nor
// List locks a row: the version guard on Save is what serializes two writers.
type LabourPlanRepo struct {
	pool *pgxpool.Pool
}

// NewLabourPlanRepo constructs a LabourPlanRepo over pool.
func NewLabourPlanRepo(pool *pgxpool.Pool) *LabourPlanRepo {
	return &LabourPlanRepo{pool: pool}
}

// labourPlanColumns is the column list scanLabourPlan expects, in order.
const labourPlanColumns = `site_id, shift_id, path_id, planned_heads, rate_units_per_hour, hours,
		installed_stations, shift_start, heads_source, version`

// Get implements ports.LabourPlanRepository.
func (r *LabourPlanRepo) Get(ctx context.Context, siteID, shiftID, pathID string) (*labourplan.Plan, bool, error) {
	p, err := scanLabourPlan(queryFor(ctx, r.pool).QueryRow(ctx, `
		SELECT `+labourPlanColumns+` FROM labour_plans WHERE site_id = $1 AND shift_id = $2 AND path_id = $3
	`, siteID, shiftID, pathID))
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, err
	}
	return p, true, nil
}

// Save implements ports.LabourPlanRepository through saveVersioned; every
// field of the spec is written.
func (r *LabourPlanRepo) Save(ctx context.Context, p *labourplan.Plan) error {
	s := p.Spec()
	return saveVersioned(ctx, queryFor(ctx, r.pool), p.Version(), `
		INSERT INTO labour_plans (`+labourPlanColumns+`)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
		ON CONFLICT (site_id, shift_id, path_id) DO NOTHING`, `
		UPDATE labour_plans SET planned_heads = $4, rate_units_per_hour = $5, hours = $6,
		       installed_stations = $7, shift_start = $8, heads_source = $9, version = $10
		WHERE site_id = $1 AND shift_id = $2 AND path_id = $3 AND version = $10 - 1`,
		s.SiteID, s.ShiftID, s.PathID, s.PlannedHeads, s.RateUnitsPerHour, s.Hours,
		s.InstalledStations, s.ShiftStart, string(s.HeadsSource), p.Version())
}

// List implements ports.LabourPlanRepository: one keyset page of (siteID,
// shiftID) by path_id (COLLATE "C", byte order, like the in-memory double),
// strictly after afterPathID. It reads limit+1 rows to learn whether more
// follow; a negative limit is 0. It returns an empty, non-nil slice when
// nothing matches.
func (r *LabourPlanRepo) List(ctx context.Context, siteID, shiftID, afterPathID string, limit int) ([]*labourplan.Plan, bool, error) {
	limit = max(limit, 0)
	rows, err := queryFor(ctx, r.pool).Query(ctx, `
		SELECT `+labourPlanColumns+` FROM labour_plans
		WHERE site_id = $1 AND shift_id = $2 AND path_id > $3
		ORDER BY path_id
		LIMIT $4`, siteID, shiftID, afterPathID, limit+1)
	if err != nil {
		return nil, false, err
	}
	defer rows.Close()

	out := make([]*labourplan.Plan, 0)
	for rows.Next() {
		p, err := scanLabourPlan(rows)
		if err != nil {
			return nil, false, err
		}
		out = append(out, p)
	}
	if err := rows.Err(); err != nil {
		return nil, false, err
	}
	more := len(out) > limit
	if more {
		out = out[:limit]
	}
	return out, more, nil
}

// scanLabourPlan rehydrates one labour_plans row selected with
// labourPlanColumns through labourplan.Rehydrate, so a corrupt row is an
// error. pgx.ErrNoRows is returned unwrapped.
func scanLabourPlan(row pgx.Row) (*labourplan.Plan, error) {
	var (
		s       labourplan.Spec
		source  string
		version int64
	)
	if err := row.Scan(&s.SiteID, &s.ShiftID, &s.PathID, &s.PlannedHeads, &s.RateUnitsPerHour, &s.Hours,
		&s.InstalledStations, &s.ShiftStart, &source, &version); err != nil {
		return nil, err
	}
	s.HeadsSource = labourplan.HeadsSource(source)
	p, err := labourplan.Rehydrate(s, version)
	if err != nil {
		return nil, fmt.Errorf("labour_plans row (%s, %s, %s): %w", s.SiteID, s.ShiftID, s.PathID, err)
	}
	return p, nil
}
