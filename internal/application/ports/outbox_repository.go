package ports

import (
	"context"

	"github.com/claudioed/warehouse-planning/internal/application/outbox"
)

// OutboxRepository is the write side of the transactional outbox. Insert
// stores already-encoded messages; called with the ctx handed to
// UnitOfWork.Do it joins the SAME database transaction as the aggregate's
// Save, so the aggregate and its integration events commit or roll back
// together -- there is no dual write. A relay adapter later drains the
// stored rows to Kafka (see internal/adapters/outbound/outbox).
type OutboxRepository interface {
	Insert(ctx context.Context, msgs ...outbox.Message) error
}
