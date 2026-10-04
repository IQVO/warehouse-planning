package postgres

import (
	"context"
	"errors"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/claudioed/warehouse-planning/internal/domain/demand"
)

// OrderDemandRepo is a pgxpool-backed ports.OrderDemandRepository over the
// order_demand table (migration 0006). Inside a ports.UnitOfWork it uses the
// transaction carried by ctx, so the consumer's processed-event claim and
// the upsert commit or roll back together.
type OrderDemandRepo struct {
	pool *pgxpool.Pool
}

// NewOrderDemandRepo constructs an OrderDemandRepo over pool.
func NewOrderDemandRepo(pool *pgxpool.Pool) *OrderDemandRepo {
	return &OrderDemandRepo{pool: pool}
}

// Upsert implements ports.OrderDemandRepository. The stored row is read
// FOR UPDATE and the replace decision is demand.Order.Supersedes -- the SAME
// function the in-memory adapter uses -- so the two cannot disagree on
// last-writer-wins.
func (r *OrderDemandRepo) Upsert(ctx context.Context, o demand.Order) (bool, error) {
	applied := false
	err := inTx(ctx, r.pool, func(q querier) error {
		var (
			location string
			promise  time.Time
			lines    int
			asOf     time.Time
		)
		err := q.QueryRow(ctx, `
			SELECT location, promise_at, released_lines, as_of
			FROM order_demand WHERE order_id = $1 FOR UPDATE
		`, o.ID()).Scan(&location, &promise, &lines, &asOf)
		switch {
		case err == nil:
			prev, perr := demand.NewOrder(demand.OrderParams{
				OrderID: o.ID(), Location: location, PromiseAt: promise, ReleasedLines: lines, AsOf: asOf,
			})
			if perr != nil {
				return perr
			}
			if !o.Supersedes(prev) {
				return nil
			}
		case !errors.Is(err, pgx.ErrNoRows):
			return err
		}
		if _, err := q.Exec(ctx, `
			INSERT INTO order_demand (order_id, location, promise_at, released_lines, as_of)
			VALUES ($1, $2, $3, $4, $5)
			ON CONFLICT (order_id) DO UPDATE SET
				location = EXCLUDED.location,
				promise_at = EXCLUDED.promise_at,
				released_lines = EXCLUDED.released_lines,
				as_of = EXCLUDED.as_of
		`, o.ID(), o.Location(), o.PromiseAt(), o.ReleasedLines(), o.AsOf()); err != nil {
			return err
		}
		applied = true
		return nil
	})
	if err != nil {
		return false, err
	}
	return applied, nil
}

// Expected implements ports.OrderDemandRepository. The window is half-open
// ([start, end)), matching demand.Order.CountsIn; as_of is the site's newest
// event time regardless of the window.
func (r *OrderDemandRepo) Expected(ctx context.Context, location string, start, end time.Time) (demand.Summary, error) {
	var (
		orders int64
		lines  int64
		asOf   *time.Time
	)
	err := queryFor(ctx, r.pool).QueryRow(ctx, `
		SELECT count(*) FILTER (WHERE promise_at >= $2 AND promise_at < $3),
		       COALESCE(sum(released_lines) FILTER (WHERE promise_at >= $2 AND promise_at < $3), 0),
		       max(as_of)
		FROM order_demand WHERE location = $1
	`, location, start, end).Scan(&orders, &lines, &asOf)
	if err != nil {
		return demand.Summary{}, err
	}
	s := demand.Summary{Orders: int(orders), ReleasedLines: int(lines)}
	if asOf != nil {
		s.AsOf = asOf.UTC()
	}
	return s, nil
}
