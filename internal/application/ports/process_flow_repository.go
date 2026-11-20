package ports

import (
	"context"

	"github.com/claudioed/warehouse-planning/internal/domain/pagecursor"
	"github.com/claudioed/warehouse-planning/internal/domain/processflow"
)

// ProcessFlowRepository is the local copy of process-path-management's process
// flows, fed only by its ProcessFlowDefined / ProcessFlowRevised /
// ProcessFlowRetired integration events (docs/adr/0025). One row per flow id.
// Nothing is ever read from process-path-management directly.
//
// Inside a UnitOfWork the repository joins the transaction carried in ctx, so
// the consumer's processed-event claim and the Record commit together.
type ProcessFlowRepository interface {
	// Record stores f under its flow id ONLY when f.Supersedes the stored
	// version (a flow not stored yet counts as version 0). applied is false
	// for an equal or lower version, which changes nothing.
	Record(ctx context.Context, f processflow.Flow) (applied bool, err error)

	// FindActive returns the ACTIVE flow stored under id, or (nil, nil) when
	// none is stored or the stored flow is RETIRED: a retired flow does not
	// resolve a path id.
	FindActive(ctx context.Context, id string) (*processflow.Flow, error)

	// ListActivePage returns at most limit ACTIVE flows ordered by flow id,
	// strictly after the cursor's ID when after is not nil (an empty, non-nil
	// slice when nothing follows).
	ListActivePage(ctx context.Context, after *pagecursor.Cursor, limit int) ([]processflow.Flow, error)
}

// ProcessFlowMetrics counts what the process flow consumer did with each
// message, by outcome (applied, stale, duplicate or rejected). A nil value is
// a no-op.
type ProcessFlowMetrics interface {
	ProcessFlowMessage(ctx context.Context, outcome string)
}

// Outcome values recorded by ProcessFlowMetrics.ProcessFlowMessage.
const (
	ProcessFlowApplied   = "applied"
	ProcessFlowStale     = "stale"
	ProcessFlowDuplicate = "duplicate"
	ProcessFlowRejected  = "rejected"
)
