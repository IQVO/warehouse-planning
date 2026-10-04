# ADR 0005: Analytics read side -- an analytics stream, a projector and read-only reports over a separate analytical database

## Status

Accepted (2026-10-04). Additive: the OLTP API, the integration topic and its
payloads are unchanged (the integration golden tests pass unchanged). Adds one
OLTP migration (`0007`, the outbox identity) and a second, separate database.

## Context

Every OLTP bounded context of the fleet ships an analytics read side
("data-mesh read side"): a separate analytical Postgres fed by a **projector**
that consumes the context's own analytics topic, and a read-only **reports**
binary over that database. `warehouse-planning` had none (its docs said "there
is no analytics stream yet"). The reference implementations are
`workforce-management`, `facility-layout` and `network-fulfillment`; this ADR
mirrors their shape and records where this service deliberately differs.

## Decision

### 1. What is projected

The four published capacity-plan events
(`com.warehouse.wes.warehouse-planning.capacityplan.{CapacityPlanCreated,
CapacityPlanPublished, CapacityShortageDetected, BottleneckDetected}`) are
ALSO written to `warehouse.warehouse-planning.analytics`, as CloudEvents 1.0
with the **same `type` and the same `id` per occurrence** as the integration
message, and `dataschema=urn:warehouse:warehouse-planning:analytics:<EventName>:v1`.
Nothing else is projected: no ProcessCapacity or demand event exists to
project, and none is invented.

The analytical model is one row per plan (`plan_facts`) assembled from those
events, plus a processed-event set. Each event upserts only the columns it
carries (a column an event does not state is left as it was), so a row
converges whatever order its events arrive in. The instants are the
CloudEvents `time` attributes: `created_at` from `CapacityPlanCreated`,
`published_at` from `CapacityPlanPublished`.

### 2. Payload additions (analytics only, additive)

Decided from the report needs, nothing speculative:

| event | addition | why |
| --- | --- | --- |
| `CapacityPlanPublished` | `binding_constraint` (the constraint type binding the bottleneck step: LABOR, STATION, ...; `""` for a plan created before it was recorded) | the bottleneck-frequency report answers *which constraint* bound the step, not only which step; `capacity_plans.bottleneck_constraint` already stores it but no event carried it |

The plan's `demand_source` (also stored) is **not** added: no report reads it.
`CapacityPlanCreated`, `CapacityShortageDetected` and `BottleneckDetected`
analytics payloads equal their integration payloads. The addition lives in
`CapacityPlanPublished.BottleneckConstraint` in the domain event (set from the
plan, no JSON in the domain) and is serialized only by the analytics encoder.
Documented in `apis/asyncapi.yaml`.

### 3. One transaction, two topics, one id

`FanoutEncoder` (what `cmd/api` and `cmd/mcp` hand the use cases) turns every
domain event into **two outbox rows, the integration row first and the
analytics row second, under one CloudEvents id minted once and persisted with
both**. The use case's single `ports.UnitOfWork` inserts them with the
aggregate, so they commit or roll back together; a relay retry republishes each
row's persisted bytes, so the id never changes. The integration encoder and its
bytes are untouched.

This needs **migration `0007_outbox_event_id_per_topic`**: `outbox_events.event_id`
was `UNIQUE` on its own, which would reject the second row. The identity of an
outbox row becomes `UNIQUE (event_id, topic)` (the same event can still never be
enqueued twice for one topic). The down migration deletes the analytics-topic
rows (a derived copy) before restoring `UNIQUE (event_id)`. Sibling repos have no
unique constraint on this column, which is why they did not need it.

### 4. The projector (`cmd/planning-projector`)

- Consumes the analytics topic under a **fixed consumer group read from env**
  (`ANALYTICS_CONSUMER_GROUP`, default `warehouse-planning-analytics`,
  set by the chart). It is an at-least-once consumer, **not** a full-replay cache:
  committed offsets are honoured, and a brand-new group starts at the earliest
  offset so the model can be rebuilt from retained history.
- **Dedupe and effect in one transaction**: `Projection.Apply` inserts the
  CloudEvents `id` into `analytics_processed_events` and upserts `plan_facts` in
  ONE analytical-database transaction. A failure leaves nothing written and the
  mark unrecorded; a replay of an applied id is a no-op. The offset is committed
  only after `Apply` returned.
- **Policy** (differs from the siblings, which dead-letter after three attempts):
  - not a CloudEvents 1.0 message (the retired flat envelope, garbage): skipped
    and committed past, with a **rate-limited WARN** (first one logged, then at
    most one per minute with the number suppressed; a previous consumer here
    logged 100k lines replaying legacy messages);
  - a valid CloudEvent of another type: acknowledged and ignored;
  - a known type with an unusable payload (missing ids, `plan_id` not the
    subject, negative shortage, no time) or one the store deterministically
    rejects (Postgres data-exception / integrity-violation class): **dead-lettered
    immediately** to `warehouse.warehouse-planning.analytics.dlq` (raw bytes plus
    `x-dlq-source-topic`, `x-dlq-error`, `x-dlq-failed-at` headers), then committed
    past; a failed DLQ write retries the message, so poison is never lost;
  - a **transient** failure (database down, timeout): the same message is retried
    with capped exponential backoff and never dead-lettered. A database outage must
    not turn into silently missing analytics; the cost is that a prolonged outage
    blocks the partition, which the logs make visible.
- Boot: migrations and the first ping under `internal/bootretry`; Kafka is dialled
  lazily by the reader. `/healthz` and `/readyz` on `:8091`; graceful shutdown
  flips readiness, drains, lets the in-flight message finish, then closes the pool.
- It writes only to the analytical database (`ANALYTICS_DATABASE_URL`, a direct
  DSN) and applies `analytics/migrations` itself, like the siblings.

### 5. The reports (`cmd/planning-reports`, `:8092`)

Read-only HTTP over the analytical database through a read-only pool
(`default_transaction_read_only=on`); `/healthz`; RFC 7807 errors; no auth (fleet
rule); additive OpenAPI (`apis/openapi.yaml`, tag `reports`). All three accept
optional `from` / `to` (RFC 3339; both omitted = the 30 days ending now; at most
366 days; `from` inclusive, `to` exclusive; an empty or inverted range is a 400).
Empty results are empty arrays, never errors. A site is `(warehouse_id, location)`;
a day is a UTC calendar day.

| endpoint | question | shape |
| --- | --- | --- |
| `GET /reports/bottleneck-frequency` | how often each bottleneck step (and binding constraint) bound a **published** plan, by site | `rows[]`: site, `bottleneck_step`, `binding_constraint`, `plans`, `share` (of the site's published plans) |
| `GET /reports/shortage-trend` | published plans with shortage and total shortage, by site per day (plans without shortage counted too) | `days[]`: day, site, `plans_published`, `plans_with_shortage`, `total_shortage`, `shortage_rate` |
| `GET /reports/plan-throughput` | plans created vs published per day, and create-to-publish latency by site | `days[]`: day, site, `plans_created`, `plans_published`; `latency[]`: site, `plans`, `median_seconds`, `p95_seconds` (continuous percentile) |
| `GET /reports/freshness` | how far the projection is behind (fleet analytics charter §4; one endpoint for all three reports, which read one projection) | `as_of` (newest applied CloudEvents time), `lag_seconds` (now - as_of, never negative); both `null` until the first event |

SQL only counts, sums and takes `percentile_cont`; rates, shares, range rules and
the percentile definition live in `internal/analytics/report` (pure, mutation
tested). The in-memory store and the Postgres store run the same contract test.

### 6. Why a separate analytical database

The projection is a derived copy that can be dropped and rebuilt from the topic;
keeping it out of the OLTP database means a heavy report cannot take OLTP
connections, the OLTP schema and the analytical schema evolve independently, the
reports role can be read-only on a database that holds nothing transactional, and
the projector (the only writer) has no credentials on OLTP data. The cost is a
second role/database to provision (`warehouse-infra`, with the usual initdb trap:
an already-populated Postgres does not re-run the init script).

### 7. Packaging

The one image builds every `cmd/*`, so the projector and reports binaries ship in
the same image (`/app/planning-projector`, `/app/planning-reports`, uid 1000) with
`analytics/migrations` in `/app/analytics/migrations`. The chart gains two
components, `analytics-projector` and `analytics-reports` (Deployment, plus a
Service for reports), behind `analytics.enabled` (default `false`: the default
render is byte-identical to before).

## Consequences

- Two outbox rows per event; the relay publishes both through the same sink.
- `analytics_*` metrics are not exposed (this service has no `/metrics` and no OTel
  exporter in these binaries); a dashboard would need a metrics surface first.
- The analytics stream is not an integration contract: only `planning-projector`
  consumes it.
- `ProcessCapacity` changes and demand ingestion are not part of this read side.

## Alternatives considered and rejected

- **Publish analytics from the integration topic** (a second consumer of
  `warehouse.warehouse-planning.events`): the integration contract would carry
  analytics-only fields, or the projector could not see them; and the siblings'
  convention is a dedicated topic.
- **Two ids per occurrence** (mint a new id for the analytics row): breaks the
  "same type and id on both topics" rule that lets a consumer correlate the two
  streams and keeps retries idempotent per occurrence.
- **Keep `UNIQUE (event_id)` and suffix the analytics id**: same objection, and the
  persisted CloudEvents `id` would no longer equal the id in the bytes.
- **Project into the OLTP database** (tables next to `capacity_plans`): no isolation
  of load, schema or credentials; a report bug could hold OLTP connections.
- **Compute the reports straight from `capacity_plans`**: the table holds the
  current state only, with no created/published history per event and no
  replayable stream; and it would put analytical queries on the OLTP database.
- **Dead-letter after N attempts for every failure** (the siblings' policy): drops
  analytics for events that failed only because the database was briefly down.
- **A full-replay cache consumer** (unique group per process): the model is a
  database, not a per-process cache; a fixed group gives at-least-once with
  committed offsets.
- **Add `demand_source` to the payload**: no report needs it today.
