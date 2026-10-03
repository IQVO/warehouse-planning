package ports

import "context"

// ProcessedEventRepository is the idempotency guard shared by every
// inbound Kafka consumer in this service (Phase 3). A consumer claims an
// event by (consumer name, CloudEvents id) BEFORE applying any side
// effect; Claim returning false means this exact event was already
// handled by this consumer and must be skipped, not reprocessed. This is
// what makes a redelivered/replayed message safe for an INCREMENT-style
// read model (the storage/station tally) where "upsert is naturally
// idempotent" does not hold -- see .claude/rules/integration-events.md.
type ProcessedEventRepository interface {
	// Claim atomically records (consumer, eventID) as processed and
	// reports whether THIS call was the one that recorded it (true) or
	// whether it was already recorded by an earlier call (false).
	Claim(ctx context.Context, consumer, eventID string) (claimed bool, err error)
}
