package analyticsstore

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/claudioed/warehouse-planning/internal/analytics/report"
)

// Reader is the Postgres READER (report.Reader). Every query filters its
// instant column with `>= from AND < to`: from inclusive, to exclusive.
// Days are UTC calendar days. Aggregation that is plain arithmetic (rates,
// shares) is left to internal/analytics/report; SQL only counts and sums,
// plus percentile_cont for the latency percentiles.
type Reader struct {
	pool *pgxpool.Pool
}

// NewReader constructs a Reader over the (read-only) reports pool.
func NewReader(pool *pgxpool.Pool) *Reader { return &Reader{pool: pool} }

var _ report.Reader = (*Reader)(nil)

const bottleneckSQL = `
	SELECT warehouse_id, location, bottleneck_step, binding_constraint, count(*)
	FROM plan_facts
	WHERE published_at >= $1 AND published_at < $2
	GROUP BY warehouse_id, location, bottleneck_step, binding_constraint
	ORDER BY warehouse_id, location, count(*) DESC, bottleneck_step, binding_constraint`

// BottleneckCounts implements report.Reader.
func (r *Reader) BottleneckCounts(ctx context.Context, rg report.Range) ([]report.BottleneckCount, error) {
	return collect(ctx, r.pool, bottleneckSQL, rg, func(rows pgx.Rows) (report.BottleneckCount, error) {
		var c report.BottleneckCount
		err := rows.Scan(&c.WarehouseID, &c.Location, &c.BottleneckStep, &c.BindingConstraint, &c.Plans)
		return c, err
	})
}

const shortageSQL = `
	SELECT (published_at AT TIME ZONE 'UTC')::date AS day, warehouse_id, location,
	       count(*), count(*) FILTER (WHERE shortage > 0), COALESCE(sum(shortage), 0)
	FROM plan_facts
	WHERE published_at >= $1 AND published_at < $2
	GROUP BY day, warehouse_id, location
	ORDER BY day, warehouse_id, location`

// ShortageDays implements report.Reader.
func (r *Reader) ShortageDays(ctx context.Context, rg report.Range) ([]report.ShortageDay, error) {
	return collect(ctx, r.pool, shortageSQL, rg, func(rows pgx.Rows) (report.ShortageDay, error) {
		var d report.ShortageDay
		err := rows.Scan(&d.Day, &d.WarehouseID, &d.Location, &d.PlansPublished, &d.PlansWithShortage, &d.TotalShortage)
		d.Day = d.Day.UTC()
		return d, err
	})
}

const throughputSQL = `
	WITH created AS (
		SELECT (created_at AT TIME ZONE 'UTC')::date AS day, warehouse_id, location, count(*) AS n
		FROM plan_facts WHERE created_at >= $1 AND created_at < $2
		GROUP BY day, warehouse_id, location
	), published AS (
		SELECT (published_at AT TIME ZONE 'UTC')::date AS day, warehouse_id, location, count(*) AS n
		FROM plan_facts WHERE published_at >= $1 AND published_at < $2
		GROUP BY day, warehouse_id, location
	)
	SELECT COALESCE(c.day, p.day), COALESCE(c.warehouse_id, p.warehouse_id), COALESCE(c.location, p.location),
	       COALESCE(c.n, 0), COALESCE(p.n, 0)
	FROM created c
	FULL OUTER JOIN published p ON p.day = c.day AND p.warehouse_id = c.warehouse_id AND p.location = c.location
	ORDER BY 1, 2, 3`

// ThroughputDays implements report.Reader.
func (r *Reader) ThroughputDays(ctx context.Context, rg report.Range) ([]report.ThroughputDay, error) {
	return collect(ctx, r.pool, throughputSQL, rg, func(rows pgx.Rows) (report.ThroughputDay, error) {
		var d report.ThroughputDay
		err := rows.Scan(&d.Day, &d.WarehouseID, &d.Location, &d.PlansCreated, &d.PlansPublished)
		d.Day = d.Day.UTC()
		return d, err
	})
}

const latencySQL = `
	SELECT warehouse_id, location, count(*),
	       percentile_cont(0.5)  WITHIN GROUP (ORDER BY secs),
	       percentile_cont(0.95) WITHIN GROUP (ORDER BY secs)
	FROM (
		SELECT warehouse_id, location, EXTRACT(EPOCH FROM (published_at - created_at))::double precision AS secs
		FROM plan_facts
		WHERE published_at >= $1 AND published_at < $2 AND created_at IS NOT NULL
	) latencies
	GROUP BY warehouse_id, location
	ORDER BY warehouse_id, location`

// Latencies implements report.Reader.
func (r *Reader) Latencies(ctx context.Context, rg report.Range) ([]report.Latency, error) {
	return collect(ctx, r.pool, latencySQL, rg, func(rows pgx.Rows) (report.Latency, error) {
		var l report.Latency
		err := rows.Scan(&l.WarehouseID, &l.Location, &l.Plans, &l.MedianSeconds, &l.P95Seconds)
		return l, err
	})
}

// collect runs query with the range as $1/$2 and scans every row; the result
// is a non-nil slice even when empty.
func collect[T any](ctx context.Context, pool *pgxpool.Pool, query string, rg report.Range, scan func(pgx.Rows) (T, error)) ([]T, error) {
	rows, err := pool.Query(ctx, query, rg.From, rg.To)
	if err != nil {
		return nil, fmt.Errorf("analyticsstore: query: %w", err)
	}
	defer rows.Close()
	out := []T{}
	for rows.Next() {
		v, err := scan(rows)
		if err != nil {
			return nil, fmt.Errorf("analyticsstore: scan: %w", err)
		}
		out = append(out, v)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("analyticsstore: rows: %w", err)
	}
	return out, nil
}

// day is the UTC calendar day of t, at midnight UTC.
func day(t time.Time) time.Time {
	y, m, d := t.UTC().Date()
	return time.Date(y, m, d, 0, 0, 0, 0, time.UTC)
}
