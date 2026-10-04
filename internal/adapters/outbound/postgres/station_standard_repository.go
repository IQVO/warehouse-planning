package postgres

import (
	"context"
	"errors"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/claudioed/warehouse-planning/internal/domain/processcapacity"
)

// StationStandardRepo is a pgxpool-backed ports.StationStandardRepository
// (table station_standards, migration 0005). Inside a ports.UnitOfWork it uses
// the transaction carried by ctx (pgtx).
type StationStandardRepo struct {
	pool *pgxpool.Pool
}

// NewStationStandardRepo constructs a StationStandardRepo over pool.
func NewStationStandardRepo(pool *pgxpool.Pool) *StationStandardRepo {
	return &StationStandardRepo{pool: pool}
}

// Save upserts standard under its (location, process_type) key; an existing
// row's throughput is replaced and updated_at refreshed.
func (r *StationStandardRepo) Save(ctx context.Context, standard processcapacity.StationStandard) error {
	rate := standard.PerStation()
	_, err := queryFor(ctx, r.pool).Exec(ctx, `
		INSERT INTO station_standards (location, process_type, quantity, unit, period_seconds, updated_at)
		VALUES ($1, $2, $3, $4, $5, now())
		ON CONFLICT (location, process_type) DO UPDATE SET
			quantity = EXCLUDED.quantity,
			unit = EXCLUDED.unit,
			period_seconds = EXCLUDED.period_seconds,
			updated_at = now()
	`, standard.Location(), string(standard.ProcessType()), rate.Quantity(), string(rate.Unit()), rate.Period().Seconds())
	return err
}

// Find returns the standard declared for location + processType, or (nil, nil).
func (r *StationStandardRepo) Find(ctx context.Context, location string, processType processcapacity.ProcessType) (*processcapacity.StationStandard, error) {
	row := queryFor(ctx, r.pool).QueryRow(ctx, `
		SELECT location, process_type, quantity, unit, period_seconds
		FROM station_standards WHERE location = $1 AND process_type = $2
	`, location, string(processType))
	standard, err := scanStationStandard(row)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, nil
		}
		return nil, err
	}
	return &standard, nil
}

// List returns the standards of location (every standard when location is
// empty) ordered by location, then process type.
func (r *StationStandardRepo) List(ctx context.Context, location string) ([]processcapacity.StationStandard, error) {
	rows, err := queryFor(ctx, r.pool).Query(ctx, `
		SELECT location, process_type, quantity, unit, period_seconds
		FROM station_standards
		WHERE $1 = '' OR location = $1
		ORDER BY location, process_type
	`, location)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	out := []processcapacity.StationStandard{}
	for rows.Next() {
		standard, err := scanStationStandard(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, standard)
	}
	return out, rows.Err()
}

func scanStationStandard(row pgx.Row) (processcapacity.StationStandard, error) {
	var (
		location, processType, unit string
		quantity, periodSeconds     float64
	)
	if err := row.Scan(&location, &processType, &quantity, &unit, &periodSeconds); err != nil {
		return processcapacity.StationStandard{}, err
	}
	rate, err := processcapacity.NewCapacityRate(quantity, processcapacity.CapacityUnit(unit), time.Duration(periodSeconds*float64(time.Second)))
	if err != nil {
		return processcapacity.StationStandard{}, err
	}
	return processcapacity.NewStationStandard(location, processcapacity.ProcessType(processType), rate)
}
