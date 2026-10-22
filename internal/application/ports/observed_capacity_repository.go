package ports

import (
	"context"
	"time"

	"github.com/claudioed/warehouse-planning/internal/domain/observed"
)

// ObservedCapacityRepository is the local observed-throughput read model fed
// by fulfillment-execution's TaskCompleted events (docs/adr/0017): completed
// tasks aggregated per (site, process step, minute). It is never read from or
// written to fulfillment-execution directly.
//
// Inside a UnitOfWork the repository joins the transaction carried in ctx, so
// the consumer's processed-event claim and the Record commit together.
type ObservedCapacityRepository interface {
	// Record adds the completion to its (location, process type, minute)
	// bucket. Exactly-once is the caller's processed-event claim, not this
	// method's: calling it twice counts twice.
	Record(ctx context.Context, c observed.Completion) error

	// Throughput sums the tasks of (location, processType) whose minute
	// bucket lies in the half-open window [start, end), with the newest
	// completion time the step's model reflects. A step with nothing
	// observed yields the zero Throughput, not an error.
	Throughput(ctx context.Context, location, processType string, start, end time.Time) (observed.Throughput, error)
}
