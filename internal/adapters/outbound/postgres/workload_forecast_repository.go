package postgres

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/claudioed/warehouse-planning/internal/application/ports"
	"github.com/claudioed/warehouse-planning/internal/domain/workloadforecast"
)

// WorkloadForecastRepo is a pgxpool-backed ports.WorkloadForecastRepository
// over the workload_forecasts table (migration 0014, docs/adr/0023). Inside a
// ports.UnitOfWork it uses the transaction carried by ctx, so its Save
// commits or rolls back with the outbox insert of the event. Get never locks
// a row: the version guard on Save is what serializes two writers.
type WorkloadForecastRepo struct {
	pool *pgxpool.Pool
}

// NewWorkloadForecastRepo constructs a WorkloadForecastRepo over pool.
func NewWorkloadForecastRepo(pool *pgxpool.Pool) *WorkloadForecastRepo {
	return &WorkloadForecastRepo{pool: pool}
}

// Get implements ports.WorkloadForecastRepository. The row is rehydrated
// through workloadforecast.Rehydrate, so a corrupt row is an error.
func (r *WorkloadForecastRepo) Get(ctx context.Context, siteID, pathID string) (*workloadforecast.Forecast, bool, error) {
	var (
		raw        []byte
		receivedAt time.Time
		version    int64
	)
	err := queryFor(ctx, r.pool).QueryRow(ctx, `
		SELECT buckets, received_at, version FROM workload_forecasts WHERE site_id = $1 AND path_id = $2
	`, siteID, pathID).Scan(&raw, &receivedAt, &version)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, err
	}
	buckets, err := decodeBuckets(raw)
	if err != nil {
		return nil, false, fmt.Errorf("workload_forecasts row (%s, %s): %w", siteID, pathID, err)
	}
	f, err := workloadforecast.Rehydrate(siteID, pathID, buckets, receivedAt, version)
	if err != nil {
		return nil, false, fmt.Errorf("workload_forecasts row (%s, %s): %w", siteID, pathID, err)
	}
	return f, true, nil
}

// Save implements ports.WorkloadForecastRepository through saveVersioned;
// the buckets are written whole.
func (r *WorkloadForecastRepo) Save(ctx context.Context, f *workloadforecast.Forecast) error {
	buckets, err := encodeBuckets(f.Buckets())
	if err != nil {
		return err
	}
	return saveVersioned(ctx, queryFor(ctx, r.pool), f.Version(), `
		INSERT INTO workload_forecasts (site_id, path_id, buckets, received_at, version)
		VALUES ($1, $2, $3::jsonb, $4, $5)
		ON CONFLICT (site_id, path_id) DO NOTHING`, `
		UPDATE workload_forecasts SET buckets = $3::jsonb, received_at = $4, version = $5
		WHERE site_id = $1 AND path_id = $2 AND version = $5 - 1`,
		f.SiteID(), f.PathID(), string(buckets), f.ReceivedAt(), f.Version())
}

// saveVersioned is the version-guarded write of the versioned repositories
// (ports.WorkloadForecastRepository, ports.LabourPlanRepository). At version
// 1 it runs insert, an INSERT ... ON CONFLICT DO NOTHING; at a later version
// update, an UPDATE whose WHERE holds the stored version at version-1. Both
// statements take the same args. Zero rows affected is
// ports.ErrVersionConflict either way; ON CONFLICT DO NOTHING (rather than a
// unique violation) keeps an enclosing unit of work's transaction usable.
func saveVersioned(ctx context.Context, q querier, version int64, insert, update string, args ...any) error {
	query := update
	if version == 1 {
		query = insert
	}
	tag, err := q.Exec(ctx, query, args...)
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		return ports.ErrVersionConflict
	}
	return nil
}

// bucketRow is one element of workload_forecasts.buckets (JSONB, migration
// 0014). The JSON shape is this adapter's storage format only; the domain
// carries no tags. Both fields are pointers so a row missing one is an error,
// never a silent zero.
type bucketRow struct {
	CPT      *time.Time `json:"cpt"`
	Quantity *int64     `json:"quantity"`
}

// errCorruptBucket is returned for a stored bucket missing its cpt or its
// quantity.
var errCorruptBucket = errors.New("a stored bucket needs both cpt and quantity")

// encodeBuckets serializes the buckets in order; the CPT keeps its
// nanoseconds (RFC 3339).
func encodeBuckets(buckets []workloadforecast.Bucket) ([]byte, error) {
	rows := make([]bucketRow, 0, len(buckets))
	for _, b := range buckets {
		cpt, quantity := b.CPT, b.Quantity
		rows = append(rows, bucketRow{CPT: &cpt, Quantity: &quantity})
	}
	return json.Marshal(rows)
}

// decodeBuckets reads the column back in order. A bucket missing a field is
// errCorruptBucket; every other invariant is workloadforecast.Rehydrate's.
func decodeBuckets(raw []byte) ([]workloadforecast.Bucket, error) {
	var rows []bucketRow
	if err := json.Unmarshal(raw, &rows); err != nil {
		return nil, fmt.Errorf("decode workload_forecasts.buckets: %w", err)
	}
	out := make([]workloadforecast.Bucket, 0, len(rows))
	for _, b := range rows {
		if b.CPT == nil || b.Quantity == nil {
			return nil, errCorruptBucket
		}
		out = append(out, workloadforecast.Bucket{CPT: *b.CPT, Quantity: *b.Quantity})
	}
	return out, nil
}
