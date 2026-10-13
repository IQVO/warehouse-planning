# ADR 0013: Outbox relay attempt cap

## Status

Accepted (2026-10-13, ecosystem gap fix D12). Amends the outbox ordering
guarantee recorded in
[ADR 0007](./0007-outbox-and-resilient-consumers.md) §1 (the relay drains
rows in `id` order and a later row never overtakes a failed earlier one):
ordering still holds for every row the relay can deliver, but a row excluded
by the cap is overtaken. Everything else in ADR 0007 and
[ADR 0005](./0005-analytics-read-side.md) (one row per event and topic,
written in the use case's transaction, drained in `id` order) is unchanged.

## Context

`cmd/api` runs the transactional-outbox relay
(`internal/adapters/outbound/outbox`). It sends rows one at a time in `id`
order and stops a pass at the first failed row, so a later row never
overtakes an earlier one. Until now there was no limit: a row that failed was
retried on every pass, forever.

That is right for a broker outage, which ends. It is wrong for a **poison
row**, one the broker will never accept (a message over the broker's size
limit, a topic that cannot be created, a value the sink rejects every time).
Such a row fails every pass, every pass stops at it, and every row behind it,
for every plan and both topics (`warehouse.warehouse-planning.events` and
`warehouse.warehouse-planning.analytics`), waits forever. Nothing pages: the
relay logs `outbox relay pass failed` per pass and the process stays healthy.

The fleet decision (gap-fix wave 2026-10-12, D12) is that every relay passes
an attempt cap, with the convention the fleet's reference relay already had:
an excluded row stays in the outbox table for inspection.

## Decision

1. **The relay takes an attempt cap.** `outboxrelay.WithMaxAttempts(n)`;
   the relay's own zero value stays `0` (unlimited) so the cap is always an
   explicit choice at the composition root. `Store.Drain` receives the cap
   and does not claim a row whose `attempts` already reached it (Postgres:
   `AND attempts < $2` on the `FOR UPDATE SKIP LOCKED` claim; the in-memory
   store applies the same filter).
2. **`cmd/api` reads `OUTBOX_MAX_ATTEMPTS`.** Default `20`
   (`outboxrelay.DefaultMaxAttempts`); `0` means unlimited, the old
   behaviour; an invalid or negative value logs a WARN and uses the default,
   so a typo cannot silently remove the cap. The chart exposes it as
   `config.outboxMaxAttempts` (rendered into the ConfigMap and the api
   Deployment's env). `cmd/mcp`, `cmd/planning-projector` and
   `cmd/planning-reports` run no relay.
3. **An excluded row is kept, not moved.** It stays in `outbox_events` with
   `published_at IS NULL`, its `attempts` and its `last_error`, for
   inspection. There is no DLQ topic: a row the broker rejects would most
   likely be rejected on a DLQ topic of the same broker too, and the table
   already is the inspectable store. Nothing deletes outbox rows
   automatically, so an excluded row is never lost.
4. **The failure that reaches the cap is logged once at ERROR**, after the
   attempt is committed, with the message
   `outbox relay: row reached the attempt cap and is excluded from future claims; ...`
   and the fields `outbox_id`, `topic`, `event_type`, `key`, `attempts`,
   `max_attempts` and `error`. A failed `Drain` returns an
   `*outbox.SendFailure` (row id, message, attempts after the failure, cause)
   so the relay can do that without another query. Every failed pass still
   logs `outbox relay pass failed`, as before.
5. **Re-arming is an operator action.** After fixing the cause,
   `UPDATE outbox_events SET attempts = 0 WHERE id = <id>;` makes the row
   claimable again on the next pass (runbook, "Outbox relay").
6. **Sensor.** The architecture fitness test `TestOutboxRelaysPassAttemptCap`
   fails when a non-test file under `cmd/` calls `NewRelay` (or the fleet
   name `NewOutboxRelay`) without a `WithMaxAttempts(...)` argument.

## Consequences

- **Per-key ordering is no longer absolute.** Once a row is excluded, later
  rows overtake it, **including rows with the same key** (the plan id): a
  consumer can see `CapacityPlanPublished` for a plan whose
  `CapacityPlanCreated` never arrived, or see a re-armed earlier row late.
  This is the deliberate trade: the cap exists so one poison row cannot stop
  the whole relay, for every plan, forever. Consumers already dedupe on the
  CloudEvents `id` (ADR 0006, ADR 0007 §2); a re-armed row is a late delivery
  of the same `id`.
- Ordering among rows that are **not** excluded is unchanged: within a pass
  the relay still stops at the first failure, so a transient failure below
  the cap never lets a later row overtake.
- With the defaults (1 s interval, cap 20) a row that fails every send is
  excluded after roughly 20 seconds of continuous failure. A broker outage
  longer than that also excludes the rows at the head of the queue, one per
  pass, as each reaches the cap: each is logged at ERROR and must be
  re-armed. Raise `OUTBOX_MAX_ATTEMPTS` (or set `0`) where long broker
  outages are expected and a stalled relay is preferable to manual re-arming.
- An excluded row is visible with
  `SELECT id, topic, event_type, key, attempts, last_error FROM outbox_events WHERE published_at IS NULL AND attempts >= 20;`
- The relay's `Store` port changed shape (`Drain` gained `maxAttempts`); both
  adapters and the test doubles were updated together.

## Verification

- `go test ./internal/adapters/outbound/outbox/ -run TestRelay_` (in-memory
  store): `TestRelay_MaxAttempts_PoisonRowExcludedThenGoodRowPublishes`,
  `TestRelay_NoCap_PoisonRowBlocksLaterRows`,
  `TestRelay_MaxAttemptsNegativeIgnoredAndDefaultIsTwenty`,
  `TestRelay_SendFailureCarriesTheRow`.
- `go test -tags=integration ./internal/adapters/outbound/outbox/ -run TestOutboxRelay_ -count=1`
  (real Postgres via testcontainers):
  `TestOutboxRelay_MaxAttempts_PoisonRowExcludedThenGoodRowPublishes`,
  `TestOutboxRelay_MaxAttempts_ReArmedRowIsClaimedAgain`,
  `TestOutboxRelay_NoCap_PoisonRowBlocksLaterRows`.
- `go test ./cmd/api/ -run 'TestOutboxMaxAttempts|TestDefaultOutboxMaxAttemptsIsTwenty'`.
- `go test ./internal/architecture/ -run OutboxRelay`.
- `bash charts/warehouse-planning/tests/render_test.sh` asserts the chart
  default (`OUTBOX_MAX_ATTEMPTS: "20"`) and that `0` renders as is.
