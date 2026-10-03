package postgres

import (
	"context"
	"errors"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/claudioed/warehouse-planning/internal/domain/processcapacity"
)

// ProcessCapacityRepo is a pgxpool-backed implementation of
// ports.ProcessCapacityRepository.
type ProcessCapacityRepo struct {
	pool *pgxpool.Pool
}

// NewProcessCapacityRepo constructs a ProcessCapacityRepo over pool.
func NewProcessCapacityRepo(pool *pgxpool.Pool) *ProcessCapacityRepo {
	return &ProcessCapacityRepo{pool: pool}
}

// Save upserts pc's identity row and replaces its full set of constraint
// rows, all inside one transaction -- the aggregate's in-memory constraint
// map is always the single source of truth for what should be persisted,
// so deleting and reinserting every constraint row is simpler and just as
// correct as a per-row diff/upsert.
func (r *ProcessCapacityRepo) Save(ctx context.Context, pc *processcapacity.ProcessCapacity) error {
	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback(ctx) }()

	start, end := pc.Window().Start(), pc.Window().End()

	_, err = tx.Exec(ctx, `
		INSERT INTO process_capacity (process_type, location, window_start, window_end, native_unit)
		VALUES ($1, $2, $3, $4, $5)
		ON CONFLICT (process_type, location, window_start, window_end)
		DO UPDATE SET native_unit = EXCLUDED.native_unit
	`, string(pc.ProcessType()), pc.Location(), start, end, string(pc.NativeUnit()))
	if err != nil {
		return err
	}

	_, err = tx.Exec(ctx, `
		DELETE FROM process_capacity_constraint
		WHERE process_type = $1 AND location = $2 AND window_start = $3 AND window_end = $4
	`, string(pc.ProcessType()), pc.Location(), start, end)
	if err != nil {
		return err
	}

	for i, entry := range pc.Constraints() {
		_, err = tx.Exec(ctx, `
			INSERT INTO process_capacity_constraint
				(process_type, location, window_start, window_end, constraint_type, quantity, period_seconds, ordinal)
			VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
		`, string(pc.ProcessType()), pc.Location(), start, end, string(entry.Type), entry.Rate.Quantity(), entry.Rate.Period().Seconds(), i)
		if err != nil {
			return err
		}
	}

	return tx.Commit(ctx)
}

// FindByProcessLocationWindow returns the ProcessCapacity for the given
// identity, or (nil, nil) if no row exists for it.
func (r *ProcessCapacityRepo) FindByProcessLocationWindow(
	ctx context.Context,
	processType processcapacity.ProcessType,
	location string,
	windowStart, windowEnd time.Time,
) (*processcapacity.ProcessCapacity, error) {
	row := r.pool.QueryRow(ctx, `
		SELECT native_unit FROM process_capacity
		WHERE process_type = $1 AND location = $2 AND window_start = $3 AND window_end = $4
	`, string(processType), location, windowStart, windowEnd)

	var nativeUnit string
	if err := row.Scan(&nativeUnit); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, nil
		}
		return nil, err
	}

	window, err := processcapacity.NewCapacityWindow(windowStart, windowEnd)
	if err != nil {
		return nil, err
	}
	pc := processcapacity.NewProcessCapacity(processType, location, window)

	rows, err := r.pool.Query(ctx, `
		SELECT constraint_type, quantity, period_seconds
		FROM process_capacity_constraint
		WHERE process_type = $1 AND location = $2 AND window_start = $3 AND window_end = $4
		ORDER BY ordinal ASC
	`, string(processType), location, windowStart, windowEnd)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	for rows.Next() {
		var (
			constraintType string
			quantity       float64
			periodSeconds  float64
		)
		if err := rows.Scan(&constraintType, &quantity, &periodSeconds); err != nil {
			return nil, err
		}
		rate, err := processcapacity.NewCapacityRate(quantity, processcapacity.CapacityUnit(nativeUnit), time.Duration(periodSeconds*float64(time.Second)))
		if err != nil {
			return nil, err
		}
		if err := pc.AddConstraint(processcapacity.ConstraintType(constraintType), rate); err != nil {
			return nil, err
		}
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	return pc, nil
}
