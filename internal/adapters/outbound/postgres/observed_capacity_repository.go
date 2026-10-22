package postgres

import (
	"context"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/claudioed/warehouse-planning/internal/domain/observed"
)

// ObservedCapacityRepo is a pgxpool-backed ports.ObservedCapacityRepository
// over the observed_capacity table (migration 0010). Inside a
// ports.UnitOfWork it uses the transaction carried by ctx, so the consumer's
// processed-event claim and the additive upsert commit or roll back together.
type ObservedCapacityRepo struct {
	pool *pgxpool.Pool
}

// NewObservedCapacityRepo constructs an ObservedCapacityRepo over pool.
func NewObservedCapacityRepo(pool *pgxpool.Pool) *ObservedCapacityRepo {
	return &ObservedCapacityRepo{pool: pool}
}

// Record implements ports.ObservedCapacityRepository: an atomic additive
// upsert of the completion's (location, process_type, minute) bucket. The
// increment happens in SQL, so two concurrent writers of one bucket both count.
func (r *ObservedCapacityRepo) Record(ctx context.Context, c observed.Completion) error {
	var duration, withDuration int64
	if d, ok := c.DurationSeconds(); ok {
		duration, withDuration = d, 1
	}
	_, err := queryFor(ctx, r.pool).Exec(ctx, `
		INSERT INTO observed_capacity (location, process_type, bucket_start, tasks_completed, duration_seconds_total, tasks_with_duration, as_of)
		VALUES ($1, $2, $3, 1, $4, $5, $6)
		ON CONFLICT (location, process_type, bucket_start) DO UPDATE SET
			tasks_completed = observed_capacity.tasks_completed + 1,
			duration_seconds_total = observed_capacity.duration_seconds_total + EXCLUDED.duration_seconds_total,
			tasks_with_duration = observed_capacity.tasks_with_duration + EXCLUDED.tasks_with_duration,
			as_of = GREATEST(observed_capacity.as_of, EXCLUDED.as_of)
	`, c.Location(), c.ProcessType(), c.Minute(), duration, withDuration, c.At())
	return err
}

// Throughput implements ports.ObservedCapacityRepository. The window is
// half-open ([start, end)) on the minute bucket, matching
// observed.Completion.CountsIn; as_of is the step's newest completion time
// regardless of the window.
func (r *ObservedCapacityRepo) Throughput(ctx context.Context, location, processType string, start, end time.Time) (observed.Throughput, error) {
	var (
		tasks, duration, withDuration int64
		asOf                          *time.Time
	)
	err := queryFor(ctx, r.pool).QueryRow(ctx, `
		SELECT COALESCE(sum(tasks_completed) FILTER (WHERE bucket_start >= $3 AND bucket_start < $4), 0),
		       COALESCE(sum(duration_seconds_total) FILTER (WHERE bucket_start >= $3 AND bucket_start < $4), 0),
		       COALESCE(sum(tasks_with_duration) FILTER (WHERE bucket_start >= $3 AND bucket_start < $4), 0),
		       max(as_of)
		FROM observed_capacity WHERE location = $1 AND process_type = $2
	`, location, processType, start, end).Scan(&tasks, &duration, &withDuration, &asOf)
	if err != nil {
		return observed.Throughput{}, err
	}
	t := observed.Throughput{Tasks: tasks, DurationSeconds: duration, TasksWithDuration: withDuration}
	if asOf != nil {
		t.AsOf = asOf.UTC()
	}
	return t, nil
}
