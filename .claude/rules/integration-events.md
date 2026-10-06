---
paths:
  - "internal/adapters/**/kafka/**"
  - "internal/adapters/outbound/events/**"
  - "apis/asyncapi*"
---

# Cross-service integration events (Kafka)

This service both PUBLISHES and CONSUMES. Publishes the four CapacityPlan
events to `warehouse.warehouse-planning.events` (Phase 4, through a
transactional outbox -- see "Publishing: the transactional outbox" below).
Consumes
labor and storage/station events from `workforce-management` and
`facility-layout` (Phase 3) to keep local read models current without ever
making a live cross-context call. Does NOT consume from
`process-path-management` — see `docs/adr/0001-...` Addendum (2026-10-03):
its `ProcessPath` carries no physical step sequence, so this context's own
`ProcessPath` is locally declared instead of Conformist-copied.

## Events: CloudEvents 1.0 is MANDATORY

Every Kafka message this service produces or consumes (integration
`warehouse.warehouse-planning.events` AND analytics
`warehouse.warehouse-planning.analytics`) is a CloudEvents 1.0 event in
structured content mode. This is a hard fleet rule, not a preference —
there is nothing to "choose" here:

- No flat envelope (`event_id`/`event_type`/`occurred_at`), no dual-write,
  no dual-read, no envelope toggle env var (`EVENT_ENVELOPE_MODE` is gone
  fleet-wide). `internal/architecture/fitness_test.go`'s
  `TestNoEventEnvelopeToggleOrFlatEnvelope` fails CI on any of them.
- Build/validate/(un)marshal with `github.com/cloudevents/sdk-go/v2/event`
  (latest v2) via ONE helper package, `internal/adapters/kafka/cloudevents/`.
  Transport stays `segmentio/kafka-go` (no sdk-go protocol/client
  packages, no hand-rolled CloudEvent structs).
- Every produced message carries the Kafka header
  `content-type: application/cloudevents+json; charset=UTF-8`
  (`cloudevents.ContentTypeHeader()`), next to the W3C trace headers
  (`traceparent`/`tracestate` stay in headers, never duplicated into
  extension attributes). Message key = aggregate id, `kafkago.Hash{}`
  balancer.
- Required attributes: `specversion=1.0`; `id` (UUID v4 minted ONCE per
  domain event and persisted with the outbox row, so redelivery carries
  the same id); `source=/warehouse/warehouse-planning`; `type`; `subject`
  (aggregate instance id, never empty); `time` (domain occurred-at, UTC);
  `datacontenttype=application/json`;
  `dataschema=urn:warehouse:warehouse-planning:<events|analytics>:<EventName>:v<N>`.
  No custom extension attributes without an ADR.
- `type` = `com.warehouse.wes.warehouse-planning.<entity>.<EventName>`
  (this context is `wes`-tier; `wms` is reserved for
  `facility-layout`/`inventory-storage` only). `<entity>` is the AGGREGATE
  that raised the event, lowercase, no separators -- `capacityplan`, never
  `capacity-plan`. The SAME `type` names the
  occurrence on both the integration and the analytics topic; `dataschema`
  names the payload shape. Breaking payload change => new `.v2` type + new
  dataschema version, never mutate an existing one.
- Consumers decode with `cloudevents.Decode` (validates), dispatch on the
  FULL `type` string (never a short name or suffix match), ignore unknown
  types, read `time`/`subject` from attributes and the payload via
  `DataAs`, dedupe on `id`, and DLQ/skip — WARN log + commit past, never
  crash, never block the partition, never fall back to parsing a legacy
  shape — anything that fails CloudEvents validation.
- Tests: a golden exact-JSON test per published `type` (all attributes +
  the `content-type` header); a legacy-flat-message-rejected test per
  consumer; Kafka integration tests via testcontainers only.

The short fleet rule is `.claude/rules/fleet/cloudevents.md` (do not edit it).
The full standard, subdomain table and the fleet's cross-service type
catalogue live in the separate warehouse-docs repository, in its
docs/strategic-design/event-standard-cloudevents.md page (not in this
repo). See also `docs/adr/0001-warehouse-planning-bounded-context.md` for why
this context never makes a live cross-context REST/MCP call instead.

### Published types

Implemented in Phase 4 (encoded by `internal/adapters/outbound/kafka/encoder.go`
through the `cloudevents` helper; golden exact-JSON tests in
`encoder_test.go`). Topic `warehouse.warehouse-planning.events`, Kafka key =
`subject` = the capacity plan id, source `/warehouse/warehouse-planning`.

| `type` | raised | `dataschema` |
| --- | --- | --- |
| `com.warehouse.wes.warehouse-planning.capacityplan.CapacityPlanCreated` | `POST /capacity-plans` | `urn:warehouse:warehouse-planning:events:CapacityPlanCreated:v1` |
| `com.warehouse.wes.warehouse-planning.capacityplan.CapacityPlanPublished` | publish | `urn:warehouse:warehouse-planning:events:CapacityPlanPublished:v1` |
| `com.warehouse.wes.warehouse-planning.capacityplan.CapacityShortageDetected` | publish, ONLY when `shortage > 0` | `urn:warehouse:warehouse-planning:events:CapacityShortageDetected:v1` |
| `com.warehouse.wes.warehouse-planning.capacityplan.BottleneckDetected` | publish, ONLY when `shortage > 0` | `urn:warehouse:warehouse-planning:events:BottleneckDetected:v1` |

Payloads (snake_case; quantities are orders; `path_capacity` is ORDER/HOUR;
times RFC 3339 UTC) are documented field by field in `apis/asyncapi.yaml`.
`CapacityShortageDetected.data` = `plan_id`, `warehouse_id`, `location`,
`path_id`, `window_start`, `window_end`, `assigned_demand`,
`capacity_over_window`, `shortage`, `bottleneck_step`.

NOT implemented (do not list them as published): `ProcessCapacityRegistered`
and `ProcessCapacityChanged` exist as domain-model vocabulary only -- nothing
raises or publishes them yet.

### The analytics stream (ADR 0005)

The same four events are ALSO written to `warehouse.warehouse-planning.analytics`
(`outboundkafka.AnalyticsTopic`), consumed ONLY by this service's own
`cmd/planning-projector` (DLQ `warehouse.warehouse-planning.analytics.dlq`). Rules:

- **Same type, same id, other dataschema.** `FanoutEncoder` (what `cmd/api` and
  `cmd/mcp` give the use cases) turns each domain event into TWO outbox rows --
  integration first, analytics second -- under ONE CloudEvents id,
  `dataschema=urn:warehouse:warehouse-planning:analytics:<EventName>:v1`. Both rows
  are inserted by the use case's single `UnitOfWork`; never write the analytics
  topic from anywhere else. `outbox_events` is unique on `(event_id, topic)`
  (migration `0007`), not on `event_id`.
- **The integration bytes never change** for analytics' sake: their golden tests
  (`encoder_test.go`) stay as they are. Analytics-only fields go ONLY into the
  analytics payload, additively, documented in `apis/asyncapi.yaml`: today
  `binding_constraint` on `CapacityPlanPublished` (the domain event carries
  `BottleneckConstraint`; only `analytics_encoder.go` serializes it). Pinned by
  `analytics_encoder_test.go` (exact JSON per event, all attributes).
- **The projector** (`internal/adapters/inbound/kafka/analytics_consumer.go`) is a
  fixed-group, at-least-once consumer: group from env `ANALYTICS_CONSUMER_GROUP`,
  dedupe on the CloudEvents `id` in the SAME analytical-database transaction as the
  write (`analyticsstore.Projection.Apply`), offset committed after success. Not
  CloudEvents -> skipped with a rate-limited WARN; other type -> ignored; known
  type with an unusable payload or a deterministic store rejection -> DLQ at once;
  transient failure -> retried on the same message, never dead-lettered.
- The analytical database is SEPARATE (`ANALYTICS_DATABASE_URL`,
  `warehouse_planning_analytics`, migrations in `analytics/migrations/`, applied by
  the projector). The OLTP domain, application layer and OLTP adapters must not
  import `internal/analytics` or `analyticsstore` (arch-test).

### Publishing: the transactional outbox

There is no dual write. `CreateCapacityPlan` and `PublishCapacityPlan` each
run ONE `ports.UnitOfWork.Do` that saves the aggregate AND inserts the
already-encoded CloudEvents into `outbox_events` (`ports.OutboxRepository`,
joining the ctx transaction exactly like the other repos). The shape mirrors
`workforce-management` (ADR 0016) -- do not invent a second outbox design:

- **Encoding happens once, inside the use case** (`ports.EventEncoder`,
  implemented by `internal/adapters/outbound/kafka.Encoder`): the CloudEvents
  `id` is minted there and persisted, so a relay retry republishes the SAME
  bytes and id. `outbox_events.event_type` stores the FULL `type` (filter
  SQL on the full string). Other columns: `event_id` (unique PER TOPIC: the
  analytics row of an occurrence shares its id), `topic`,
  `subject`, `key`, `dataschema`, `value` (the encoded bytes), `headers`
  (JSONB, always incl. `content-type`), `created_at`, `published_at`,
  `attempts`, `last_error`.
- **Relay** (`internal/adapters/outbound/outbox`, started in `cmd/api` next to
  the consumers): drains every `OUTBOX_RELAY_INTERVAL` (default `1s`; a full
  batch of 100 is followed immediately by another pass), claims rows
  `FOR UPDATE SKIP LOCKED` in id order, sends ONE AT A TIME, marks each row
  published, stops at the first failure (a later event for a plan never
  overtakes an earlier one) and records `last_error`. Delivery is
  at-least-once: a crash between the broker ack and the UPDATE republishes
  the row, same id -- consumers dedupe on `id`.
- **`EVENT_PUBLISHER=kafka|log`** (default `log`): `kafka` writes to
  `KAFKA_BROKERS` (required in that mode); `log` logs each message and marks
  it published, so tests and local dev need no broker (and, in `log` mode,
  nothing reaches Kafka). Unknown values refuse to boot. With no
  `DATABASE_URL` the outbox is the in-memory one (same relay, same modes).
- **Kafka writer** (`RelaySink`): topic-less writer routing by
  `Message.Topic`, `RequiredAcks: RequireAll`, `BatchTimeout: 10ms`,
  `kafkago.Hash{}` balancer on the plan id key, `AllowAutoTopicCreation`
  (kafka-go retries a not-ready leader inside `WriteMessages`). Kafka is
  dialled LAZILY by the relay's first send, never at boot: a broker outage
  cannot crash the process.
- Atomicity, retry-same-id and delivery are proven in
  `postgres/capacity_plan_outbox_integration_test.go` and
  `outbox/relay_integration_test.go` (testcontainers Postgres + Kafka).

### Consumed types

Confirmed 2026-10-03 against each producer's own `apis/asyncapi.yaml` on
`origin/develop` (never guessed — see `docs/adr/0001-...` Addendum).

| `type` | topic | producer | fields used |
| --- | --- | --- | --- |
| `com.warehouse.wes.workforce-management.shiftplan.ShiftPlanCommitted` | `warehouse.workforce.events` | workforce-management | `path_id`, `planned_heads`, `planned_rate`, `planned_hours` (fan-out: one message per PathPlan line) -> LABOR `CapacityConstraint` = `planned_heads * planned_rate`; window = `[event.time, event.time + planned_hours]` (documented assumption, no real shift-start field exists yet) |
| `com.warehouse.wms.facility-layout.locationslot.LocationSlotRegistered` | `warehouse.facility.events` | facility-layout | `zoneId`, `locationType`, `role` (default `Storage`), `activities` (present only when `role=WorkCenter`) -> tallied per `(zoneId, locationType)` as storage positions (role=Storage), or per zone+activity as stations (role=WorkCenter). Tally only: no ProcessCapacity constraint is registered |
| `com.warehouse.wms.facility-layout.locationslot.LocationSlotDecommissioned` | `warehouse.facility.events` | facility-layout | `locationCode` -> decrements the same tally |
| `com.warehouse.wes.order-management.order.OrderAllocated` | `warehouse.order-management.events` | order-management | `order_id` (= `subject`), `promise_date`, `len(lines)` -> upserts ONE row per order id in the expected-demand read model (`order_demand`), site = the configured `DEMAND_SITE_ID` (the event carries none), last writer wins on the CloudEvents `time`. Consumer group env `DEMAND_CONSUMER_GROUP`: **unset = consumer OFF** (docs/adr/0004) |
| `com.warehouse.wes.order-management.order.OrderPartiallyAllocated` | `warehouse.order-management.events` | order-management | same payload shape and effect as `OrderAllocated` |

`OrderRepromised` (same topic: cpt ids only, no new cutoff instant), the
analytics-topic-only types (`OrderCancelled`, `OrderReceived`, ...) and every
other `type` are ignored. Demand is therefore counted in ORDERS (no units),
attributed to one site, and NOT netted for cancellations; an order is demand in
`[start, end)` when its `promise_date` is `>= start` and `< end`. The consumer
(`order_demand_consumer.go`) has the labor/storage consumers' exact shape: one
`UnitOfWork` for the processed-event claim (`order-demand-consumer`) AND the
upsert, offsets committed after success, transient errors retried, validation
before the transaction, never blocking the partition. `cmd/mcp` reads the table
and never dials Kafka.

`process-path-management` is deliberately NOT consumed — its `ProcessPath`
carries `path_id`/`required_capabilities`/`eligibility`, never an ordered
step sequence, so there is nothing structural to sync.

### Keying decisions

The two consumers feed this context differently, and since ADR 0002
(`docs/adr/0002-station-capacity-composition.md`) neither derives anything it
cannot derive honestly.

- **LABOR** (`ShiftPlanCommitted`, the labor consumer -- unchanged): a clean
  fit onto `ProcessCapacity`'s `(ProcessType, Location, CapacityWindow)`
  identity. `ProcessType` = uppercase(`path_id`), `Location` = `building_id`
  (the SITE code, e.g. `SIM1`), `CapacityWindow` = `[event.time, event.time +
  planned_hours)`. The window derivation is kept as is: one commit's lines
  share the start and differ in end per path (live `SIM1`: PICK +32h, REBIN
  +8h, PACK +24h), which is why capacity lookups match by window COVERAGE
  (docs/adr/0003). Rate = `planned_heads * planned_rate` registered as
  `UNIT/HOUR` (documented default -- `planned_rate`'s native unit is not
  specified upstream). This is the ONLY thing the consumers register as a
  `ProcessCapacity` constraint.
- **Stations and storage positions** (`LocationSlotRegistered` /
  `Decommissioned`, the facility consumer): a **pure tally maintainer**. It
  claims the event and mutates `location_slot_tally` /
  `location_slot_registration` inside ONE unit of work and does nothing else:
  no `ProcessCapacity` is registered, there is no sentinel `ProcessType`, no
  standing window and no fake unit. A count of positions or stations is not a
  throughput. (The old Phase 3 design -- a `STORAGE` sentinel process type with
  `Location = <zoneId>:<locationType>`, and STATION constraints in unit `LINE`
  on a `[1970, 2070)` "standing window" keyed by zone -- never combined with
  LABOR and is retired; migration `0005` deletes the rows it left behind.)
  - **Stations -> capacity, at read time.** A planning `location` is a
    site/building code (the labor consumer's `building_id`), and the zones of
    a site are the tally zones whose `zoneId` starts with `<location>-`
    (facility-layout's `LocationCode` grammar is `Site-Area-Zone-...` and its
    `SiteCode` is upper-case alphanumeric with no dashes, so the first segment
    of a zone id IS the site code; a zone whose id has no matching site simply
    does not contribute). For a path step with process `P` at location `L`,
    `GetProcessPathCapacity` / `CreateCapacityPlan` add a derived STATION
    candidate `stationCount(L, activity=P) x StationStandard(L, P)` to the
    constraints of the aggregates of `(P, L)` whose window COVERS the requested
    window (docs/adr/0003: per constraint type the latest window start wins),
    normalize every candidate to
    ORDER/hour and take the minimum (the binding constraint type is
    reported). The standard -- throughput of ONE station, e.g. 180
    PACKAGE/hour -- is an operator-declared planning parameter of THIS context
    (`PUT /station-standards/{location}/{process_type}`); no upstream
    publishes it. Stations tallied with no standard declared produce a warning
    and no invented throughput.
  - **Storage positions -> read model.** Positions per `(zoneId,
    locationType)` and stations per `(zoneId, activity)` are exposed by
    `GET /storage-capacity?location=` / `get_storage_capacity`. They are not
    process throughput and have no "consumed" figure (stock is never read from
    inventory-storage).

### Delivery guarantee and idempotency (processed_events)

Inbound consumers are **at-least-once with an atomic effect**. Three pieces
make that true; none of them works without the other two:

1. **Offsets are committed only after success.** The run loops use
   kafka-go's `FetchMessage` + `CommitMessages` (never `ReadMessage`, which
   with a `GroupID` auto-commits the offset before the message is handled).
   `CommitInterval` stays unset so the commit is synchronous. The loop
   commits a message's offset only after `HandleMessage` returned `nil`;
   on a non-nil error it does NOT commit and retries the SAME message with
   capped exponential backoff (200ms doubling to 5s, ctx-cancellable). It
   never skips a message on a transient error. If the process dies first
   the offset is uncommitted and Kafka redelivers.
2. **The processed-mark is atomic with the work.** Per message,
   `HandleMessage` runs ONE `ports.UnitOfWork.Do`: the
   `ProcessedEventRepository.Claim(consumer, CloudEvents id)` (an
   `INSERT ... ON CONFLICT DO NOTHING` whose affected-row count says
   whether this event was already handled) and every side effect (the
   facility consumer's tally mutation; the labor consumer's
   `ProcessCapacity` constraint upsert) commit or roll back together. The Postgres `UnitOfWork` carries a pgx
   transaction in the `ctx` (`postgres/pgtx`); every repo
   (`ProcessCapacityRepo`, `ProcessedEventRepo`, `StorageTallyRepo`) uses
   it when present and begins its own only when there is none (REST
   paths). `Claim` therefore keeps its semantics but is safe: a rolled-back
   handling un-claims, so the redelivery is processed instead of skipped.
   NEVER claim in its own statement/autocommit before the work -- a failure
   after that claim would make every redelivery a silent "already
   processed" (data loss). A concurrent duplicate blocks on the unique
   index until the other transaction ends, then skips.
3. **Errors are classified.** `HandleMessage` returns non-nil ONLY for
   transient/infrastructure failures (begin/commit, claim, tally, repo
   Find/Save). It returns `nil` for deterministic problems -- not a
   CloudEvent, unknown `type`, malformed payload, missing fields, duplicate
   id, untracked decommission, and (labor consumer) domain-validation
   rejections -- because retrying cannot help. Domain rejections are recognised by
   `usecases.IsDomainValidationError` (an allow-list of
   `ErrInvalidWindow`/`ErrNegativeQuantity`/`ErrNonPositivePeriod`/
   `ErrUnitMismatch`); anything NOT on that list is treated as
   infrastructure (retry, never data loss). A skipped domain rejection does
   not roll back the rest of the message, and its claim is committed so the
   event is not redelivered forever. Pure payload validation runs before
   the transaction opens, so a bad message never touches the database. Add
   any new domain sentinel `RegisterProcessCapacityConstraint` can return
   to that list. (The facility consumer registers no constraint, so it has
   no domain rejections at all.)

Why it matters most for the storage/station tally: it is an
INCREMENT/DECREMENT, not an overwrite, so "redelivery is naturally
idempotent" does not hold the way it does for LABOR's overwrite-style
`AddConstraint` -- a redelivered `LocationSlotRegistered` without the claim
would double-count a real slot. The tally's own "locationCode already
registered" no-op (`RegisterSlot` returns nil updates) is only a second
line of defence against a producer re-emitting a slot under a NEW event id;
it is safe under redelivery BECAUSE the claim and the tally commit together
(an attempt that failed after the tally step rolled the registration back
too, so the retry registers for real and cannot hit the no-op).

Dead-lettering: a TRANSIENT failure is retried a bounded number of times
with capped backoff and then published to `<topic>.dlq` with `x-dlq-*`
headers (`internal/adapters/inbound/kafka/deadletter.go`), so it no longer
wedges the partition forever. Deterministic problems (not CloudEvents, unknown
type, malformed payload, domain validation failure) return nil from
`HandleMessage` and are committed past, never retried. The analytics consumer
is the deliberate exception: it never dead-letters a transient failure
(ADR 0005 Decision 4), because losing analytics to a brief database outage is
unacceptable there. Silent drops of real data remain forbidden.

Tests that pin this: `internal/adapters/inbound/kafka/atomic_*_test.go`
(rollback of the tally mutation + claim when a failure is injected AFTER
the mutation was applied, error classification; the labor consumer's
constraint + claim), `consume_loop_test.go` / `run_loop_test.go` (commit
ordering and backoff), `atomic_consumers_integration_test.go` (real
Postgres + Kafka: rollback, retry, redelivery) and
`postgres/unit_of_work_integration_test.go`.

## Consumer group id

Every Kafka consumer group id MUST come from an env var, never a hardcoded
string literal — `internal/architecture/fitness_test.go`'s
`TestKafkaConsumerGroupNeverHardcodedInline` enforces this.
