package ports

import (
	"context"
	"time"

	"github.com/claudioed/warehouse-planning/internal/domain/laborstandard"
)

// LaborStandardRepository is the local copy of labor-performance's engineered
// labour standards, fed only by its LaborStandardDefined / LaborStandardRevised
// integration events (docs/adr/0022). One row per labor-performance standard
// id; it is never read from or written to labor-performance directly.
//
// Inside a UnitOfWork the repository joins the transaction carried in ctx, so
// the consumer's processed-event claim and the Record commit together.
type LaborStandardRepository interface {
	// Record stores s under its standard id. The same id again (a redelivery
	// under a new CloudEvents id, a replay) changes nothing: recorded is
	// false.
	Record(ctx context.Context, s laborstandard.Standard) (recorded bool, err error)

	// InForce returns the standard that applies to a plan of site siteCode,
	// task type taskType and a window starting at windowStart, decided by
	// laborstandard.Resolve over the stored records; found is false when no
	// standard applies.
	InForce(ctx context.Context, siteCode, taskType string, windowStart time.Time) (s laborstandard.Standard, found bool, err error)
}
