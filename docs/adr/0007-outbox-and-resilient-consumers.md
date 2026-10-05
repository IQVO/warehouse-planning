# ADR 0007: Transactional outbox and resilient, idempotent Kafka consumers (adoption)

## Status

Accepted (2026-10-05). Records decisions already in effect since Phase 3/4
with no ADR of their own, found by the 2026-10-04 ADR-conformance audit.
No behaviour changes here beyond §3's dead-letter fix for the three domain
consumers (storage/labor/order-demand), a real bug this phase of work
found and closed (a transient failure previously retried the same message
forever instead of being bounded and dead-lettered).

## Context

Every write this service makes to Kafka goes through a transactional outbox
(`internal/application/outbox`, `internal/adapters/outbound/outbox/relay.go`,
migration `0003_capacity_plan_and_outbox`), and every inbound Kafka consumer
is idempotent via a `processed_events` claim (migration
`0002_kafka_read_models.up.sql`) with capped-backoff retry
(`internal/adapters/inbound/kafka/kafka.go`'s `consumeLoop`). Both patterns
are the fleet standard (see `inventory-storage`/`order-management`/
`fulfillment-execution` on `origin/develop`) but were never written down for
this service.

## Decision

### 1. Transactional outbox (writer side)

A use case that raises domain events (`CreateCapacityPlan`,
`PublishCapacityPlan`) inserts the encoded CloudEvents rows into
`outbox_events` inside the SAME `ports.UnitOfWork.Do` transaction that saves
the aggregate. The aggregate and its events therefore commit or roll back
together -- there is no window where a saved plan has no corresponding
outbox row, or vice versa.

A separate goroutine, the outbox relay (`internal/adapters/outbound/outbox/
relay.go`, started by `cmd/api`'s `startOutboxRelay`), polls `outbox_events`
on a fixed interval (`OUTBOX_RELAY_INTERVAL`, default 1s) and publishes
un-relayed rows through a `Sink` (the Kafka writer in production,
`LogSink` when `EVENT_PUBLISHER` is unset -- the default for tests and local
dev). A relay retry after a crash republishes the SAME persisted bytes under
the SAME CloudEvents id, so a downstream consumer's own idempotency guard
(see below) absorbs the duplicate.

### 2. Idempotent, at-least-once Kafka consumers (reader side)

Every inbound consumer (`StorageCapacityConsumer`, `LaborCapacityConsumer`,
`OrderDemandConsumer`, `AnalyticsConsumer`) claims the incoming CloudEvents
`id` in `processed_events` (migration `0002`) inside the SAME
`ports.UnitOfWork` transaction as the side effect it drives (a tally mutation,
a `ProcessCapacity` upsert, a demand-model write). An already-claimed id is a
no-op: at-least-once Kafka delivery can never double-apply an effect.

### 3. Consumer resilience: commit-after-success, capped backoff, bootretry

- `consumeLoop` (`internal/adapters/inbound/kafka/kafka.go`) fetches one
  message at a time and does **not** advance the Kafka consumer-group offset
  until `HandleMessage` returned nil: a crash mid-handling redelivers the
  same message, never silently skipped.
- A transient (infrastructure) failure retries the SAME message with capped
  exponential backoff, 200ms doubling to a 5s ceiling, up to
  `domainMaxHandlerAttempts` (5) attempts for the three domain consumers
  (storage/labor/order-demand); once exhausted the message is published to
  `<topic>.dlq` with `x-dlq-*` headers (`internal/adapters/inbound/kafka/
  deadletter.go`) rather than blocking the partition forever -- the fix for
  a real bug this phase of work found (no DLQ existed at all for these three
  consumers; a stuck transient failure wedged the partition indefinitely).
  A deterministic problem (not CloudEvents 1.0, unknown type, malformed
  payload, missing fields, domain-validation rejection) returns nil at
  once: retrying it can never succeed, so it is skipped-and-committed, never
  retried or dead-lettered.
- `AnalyticsConsumer` (`internal/adapters/inbound/kafka/
  analytics_consumer.go`) has its OWN, deliberately different policy for
  transient failures (never dead-lettered, retried forever -- ADR 0005 §4):
  losing analytics to a brief database outage is unacceptable there in a way
  it is not for the domain consumers above.
- `internal/bootretry` wraps the first Postgres ping and the migration run at
  boot in a bounded retry (~31s): the first outbound dial of a freshly
  injected pod is reset by the service mesh's native sidecar ~10s after
  start, so a cold boot must tolerate one connection reset without the
  process exiting.

## Consequences

- A reviewer reading this service's Kafka code now has an ADR to cite
  instead of re-deriving the pattern from `inventory-storage`/
  `order-management` by analogy, including the dead-letter fix in §3.
- `AnalyticsConsumer`'s own, deliberately different policy (ADR 0005 §4) is
  unaffected by this ADR.

## Alternatives considered and rejected

- **A flat (non-transactional) event log + best-effort publish**: the
  fleet's established failure mode this avoids is "aggregate saved, event
  lost" or the reverse; the single-transaction outbox insert closes that gap
  for the cost of one extra table.
- **Auto-commit offsets (`ReadMessage` instead of `FetchMessage`+
  `CommitMessages`)**: kafka-go's `ReadMessage` with a `GroupID` commits the
  moment it returns, before handling -- a crash mid-handling would silently
  skip the message on restart.
