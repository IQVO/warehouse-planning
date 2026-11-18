package postgres

import (
	"context"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/claudioed/warehouse-planning/internal/domain/laborstandard"
)

// LaborStandardRepo is a pgxpool-backed ports.LaborStandardRepository over the
// labor_standards table (migration 0013): the local copy of labor-performance's
// engineered standards, one row per standard id (docs/adr/0022). Inside a
// ports.UnitOfWork it uses the transaction carried by ctx, so the consumer's
// processed-event claim and the Record commit or roll back together.
type LaborStandardRepo struct {
	pool *pgxpool.Pool
}

// NewLaborStandardRepo constructs a LaborStandardRepo over pool.
func NewLaborStandardRepo(pool *pgxpool.Pool) *LaborStandardRepo {
	return &LaborStandardRepo{pool: pool}
}

// Record implements ports.LaborStandardRepository: INSERT ... ON CONFLICT
// (standard_id) DO NOTHING, so a standard id already stored changes nothing
// and the affected-row count says whether this call stored it.
func (r *LaborStandardRepo) Record(ctx context.Context, s laborstandard.Standard) (bool, error) {
	tag, err := queryFor(ctx, r.pool).Exec(ctx, `
		INSERT INTO labor_standards (standard_id, task_type, handling_unit, site_code, expected_seconds,
		                             travel_component_seconds, effective_from, as_of)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
		ON CONFLICT (standard_id) DO NOTHING
	`, s.ID(), s.TaskType(), string(s.HandlingUnit()), s.SiteCode(), s.ExpectedSeconds(),
		s.TravelComponentSeconds(), s.EffectiveFrom(), s.AsOf())
	if err != nil {
		return false, err
	}
	return tag.RowsAffected() == 1, nil
}

// InForce implements ports.LaborStandardRepository. The SQL narrows the rows
// to the candidates (the task type, task-type-only, the plan's site or every
// site, in force at the window start); the decision is laborstandard.Resolve,
// the SAME function the in-memory adapter applies.
func (r *LaborStandardRepo) InForce(ctx context.Context, siteCode, taskType string, windowStart time.Time) (laborstandard.Standard, bool, error) {
	rows, err := queryFor(ctx, r.pool).Query(ctx, `
		SELECT standard_id, task_type, handling_unit, site_code, expected_seconds,
		       travel_component_seconds, effective_from, as_of
		FROM labor_standards
		WHERE task_type = $1 AND handling_unit = '' AND site_code IN ($2, '') AND effective_from <= $3
	`, taskType, siteCode, windowStart)
	if err != nil {
		return laborstandard.Standard{}, false, err
	}
	defer rows.Close()

	var candidates []laborstandard.Standard
	for rows.Next() {
		var (
			p            laborstandard.Params
			handlingUnit string
		)
		if err := rows.Scan(&p.StandardID, &p.TaskType, &handlingUnit, &p.SiteCode, &p.ExpectedSeconds,
			&p.TravelComponentSeconds, &p.EffectiveFrom, &p.AsOf); err != nil {
			return laborstandard.Standard{}, false, err
		}
		p.HandlingUnit = laborstandard.HandlingUnit(handlingUnit)
		s, err := laborstandard.New(p)
		if err != nil {
			return laborstandard.Standard{}, false, err
		}
		candidates = append(candidates, s)
	}
	if err := rows.Err(); err != nil {
		return laborstandard.Standard{}, false, err
	}
	s, ok := laborstandard.Resolve(candidates, siteCode, taskType, windowStart)
	return s, ok, nil
}
