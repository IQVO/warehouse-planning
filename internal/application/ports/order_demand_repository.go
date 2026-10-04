package ports

import (
	"context"
	"time"

	"github.com/claudioed/warehouse-planning/internal/domain/demand"
)

// OrderDemandRepository is the local expected-demand read model fed by
// order-management's published order events (docs/adr/0004). One row per
// order id, last-writer-wins on the event time; it is never read from or
// written to order-management directly.
//
// Inside a UnitOfWork the repository joins the transaction carried in ctx,
// so the consumer's processed-event claim and the Upsert commit together.
type OrderDemandRepository interface {
	// Upsert stores o under its order id when o supersedes the stored order
	// (demand.Order.Supersedes: a later or equal event time replaces, an
	// older one is a no-op) or when the id is new. It reports whether the
	// model changed.
	Upsert(ctx context.Context, o demand.Order) (applied bool, err error)

	// Expected summarizes the orders of the site `location` whose promise
	// cutoff falls in the half-open window [start, end), together with the
	// newest event time the site's model reflects. A site with no orders
	// yields the zero Summary, not an error.
	Expected(ctx context.Context, location string, start, end time.Time) (demand.Summary, error)
}
