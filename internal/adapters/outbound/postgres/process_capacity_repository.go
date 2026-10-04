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
// rows, all inside one transaction -- the one carried by ctx when the call
// runs inside a ports.UnitOfWork (it then neither begins nor commits its
// own), otherwise its own. The aggregate's in-memory constraint
// map is always the single source of truth for what should be persisted,
// so deleting and reinserting every constraint row is simpler and just as
// correct as a per-row diff/upsert.
func (r *ProcessCapacityRepo) Save(ctx context.Context, pc *processcapacity.ProcessCapacity) error {
	return inTx(ctx, r.pool, func(q querier) error {
		return savePC(ctx, q, pc)
	})
}

func savePC(ctx context.Context, q querier, pc *processcapacity.ProcessCapacity) error {
	start, end := pc.Window().Start(), pc.Window().End()

	_, err := q.Exec(ctx, `
		INSERT INTO process_capacity (process_type, location, window_start, window_end, native_unit)
		VALUES ($1, $2, $3, $4, $5)
		ON CONFLICT (process_type, location, window_start, window_end)
		DO UPDATE SET native_unit = EXCLUDED.native_unit
	`, string(pc.ProcessType()), pc.Location(), start, end, string(pc.NativeUnit()))
	if err != nil {
		return err
	}

	_, err = q.Exec(ctx, `
		DELETE FROM process_capacity_constraint
		WHERE process_type = $1 AND location = $2 AND window_start = $3 AND window_end = $4
	`, string(pc.ProcessType()), pc.Location(), start, end)
	if err != nil {
		return err
	}

	for i, entry := range pc.Constraints() {
		_, err = q.Exec(ctx, `
			INSERT INTO process_capacity_constraint
				(process_type, location, window_start, window_end, constraint_type, quantity, period_seconds, ordinal)
			VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
		`, string(pc.ProcessType()), pc.Location(), start, end, string(entry.Type), entry.Rate.Quantity(), entry.Rate.Period().Seconds(), i)
		if err != nil {
			return err
		}
	}
	return nil
}

// FindByProcessLocationWindow returns the ProcessCapacity for EXACTLY the
// given identity, or (nil, nil) if no row exists for it.
func (r *ProcessCapacityRepo) FindByProcessLocationWindow(
	ctx context.Context,
	processType processcapacity.ProcessType,
	location string,
	windowStart, windowEnd time.Time,
) (*processcapacity.ProcessCapacity, error) {
	q := queryFor(ctx, r.pool)
	row := q.QueryRow(ctx, `
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
	return loadPC(ctx, q, processType, location, windowStart, windowEnd, nativeUnit)
}

// FindCovering returns every ProcessCapacity of (process, location) whose
// window covers [windowStart, windowEnd) -- window_start <= windowStart AND
// window_end >= windowEnd -- newest first (ORDER BY window_start DESC,
// window_end ASC, the order processcapacity.SortNewestFirst defines). The
// matching headers are read and the cursor closed BEFORE any constraint query
// runs: inside a UnitOfWork every query shares one connection, which cannot
// interleave a second query with an open result set.
func (r *ProcessCapacityRepo) FindCovering(
	ctx context.Context,
	processType processcapacity.ProcessType,
	location string,
	windowStart, windowEnd time.Time,
) ([]*processcapacity.ProcessCapacity, error) {
	if _, err := processcapacity.NewCapacityWindow(windowStart, windowEnd); err != nil {
		return nil, err
	}
	q := queryFor(ctx, r.pool)
	rows, err := q.Query(ctx, `
		SELECT window_start, window_end, native_unit FROM process_capacity
		WHERE process_type = $1 AND location = $2 AND window_start <= $3 AND window_end >= $4
		ORDER BY window_start DESC, window_end ASC
	`, string(processType), location, windowStart, windowEnd)
	if err != nil {
		return nil, err
	}
	type header struct {
		start, end time.Time
		nativeUnit string
	}
	var headers []header
	for rows.Next() {
		var h header
		if err := rows.Scan(&h.start, &h.end, &h.nativeUnit); err != nil {
			rows.Close()
			return nil, err
		}
		headers = append(headers, h)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return nil, err
	}

	covering := make([]*processcapacity.ProcessCapacity, 0, len(headers))
	for _, h := range headers {
		pc, err := loadPC(ctx, q, processType, location, h.start, h.end, h.nativeUnit)
		if err != nil {
			return nil, err
		}
		covering = append(covering, pc)
	}
	return covering, nil
}

// loadPC rebuilds the aggregate identified by (processType, location, window)
// from its constraint rows, in registration order.
func loadPC(
	ctx context.Context,
	q querier,
	processType processcapacity.ProcessType,
	location string,
	windowStart, windowEnd time.Time,
	nativeUnit string,
) (*processcapacity.ProcessCapacity, error) {
	window, err := processcapacity.NewCapacityWindow(windowStart, windowEnd)
	if err != nil {
		return nil, err
	}
	pc := processcapacity.NewProcessCapacity(processType, location, window)

	rows, err := q.Query(ctx, `
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
