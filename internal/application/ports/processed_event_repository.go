package ports

import "context"

// ProcessedEventRepository is the idempotency guard shared by every
// inbound Kafka consumer in this service (Phase 3). A consumer claims an
// event by (consumer name, CloudEvents id) INSIDE the same UnitOfWork.Do
// as the event's side effects, so the claim commits or rolls back WITH
// them (a rolled-back handling un-claims; never claim in its own
// autocommit before the work). Claim returning false means a previous,
// committed handling exists and this event must be skipped, not
// reprocessed. This is
// what makes a redelivered/replayed message safe for an INCREMENT-style
// read model (the storage/station tally) where "upsert is naturally
// idempotent" does not hold -- see .claude/rules/integration-events.md.
type ProcessedEventRepository interface {
	// Claim records (consumer, eventID) as processed and reports whether
	// THIS call was the one that recorded it (true) or whether it was
	// already recorded by an earlier committed call (false). Whether the
	// record survives is decided by the surrounding UnitOfWork.
	Claim(ctx context.Context, consumer, eventID string) (claimed bool, err error)
}
