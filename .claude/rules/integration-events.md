# Cross-service integration events (Kafka)

This service both PUBLISHES and CONSUMES. Publishes capacity-plan/shortage
events to `warehouse.warehouse-planning.events` (Phase 1+4). Consumes
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
  `facility-layout`/`inventory-storage` only). The SAME `type` names the
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

Full standard, subdomain table and the fleet's cross-service type
catalogue: warehouse-docs `docs/strategic-design/event-standard-cloudevents.md`.
See also `docs/adr/0001-warehouse-planning-bounded-context.md` for why this
context never makes a live cross-context REST/MCP call instead.

### Published types

| `type` | topic(s) | `subject` | `dataschema` |
| --- | --- | --- | --- |
| `com.warehouse.wes.warehouse-planning.process-capacity.ProcessCapacityRegistered` | `warehouse.warehouse-planning.events` | `<processType>:<location>:<windowStart>` | `urn:warehouse:warehouse-planning:events:ProcessCapacityRegistered:v1` |
| `com.warehouse.wes.warehouse-planning.process-capacity.ProcessCapacityChanged` | `warehouse.warehouse-planning.events` | `<processType>:<location>:<windowStart>` | `urn:warehouse:warehouse-planning:events:ProcessCapacityChanged:v1` |
| `com.warehouse.wes.warehouse-planning.capacity-plan.CapacityPlanCreated` | `warehouse.warehouse-planning.events` | capacity plan id | `urn:warehouse:warehouse-planning:events:CapacityPlanCreated:v1` |
| `com.warehouse.wes.warehouse-planning.capacity-plan.CapacityPlanPublished` | `warehouse.warehouse-planning.events` | capacity plan id | `urn:warehouse:warehouse-planning:events:CapacityPlanPublished:v1` |
| `com.warehouse.wes.warehouse-planning.capacity-plan.CapacityShortageDetected` | `warehouse.warehouse-planning.events` | capacity plan id | `urn:warehouse:warehouse-planning:events:CapacityShortageDetected:v1` |
| `com.warehouse.wes.warehouse-planning.capacity-plan.BottleneckDetected` | `warehouse.warehouse-planning.events` | capacity plan id | `urn:warehouse:warehouse-planning:events:BottleneckDetected:v1` |

(Rows above are Phase 1/4 design intent, not yet implemented — remove this
parenthetical once each event actually ships, in the same PR.)

### Consumed types

Confirmed 2026-10-03 against each producer's own `apis/asyncapi.yaml` on
`origin/develop` (never guessed — see `docs/adr/0001-...` Addendum).

| `type` | topic | producer | fields used |
| --- | --- | --- | --- |
| `com.warehouse.wes.workforce-management.shiftplan.ShiftPlanCommitted` | `warehouse.workforce.events` | workforce-management | `path_id`, `planned_heads`, `planned_rate`, `planned_hours` (fan-out: one message per PathPlan line) -> LABOR `CapacityConstraint` = `planned_heads * planned_rate`; window = `[event.time, event.time + planned_hours]` (documented assumption, no real shift-start field exists yet) |
| `com.warehouse.wms.facility-layout.locationslot.LocationSlotRegistered` | `warehouse.facility.events` | facility-layout | `zoneId`, `locationType`, `role` (default `Storage`), `activities` (present only when `role=WorkCenter`) -> tallied per `(zoneId, locationType)` for a LOCATION constraint (role=Storage), or per zone+activity for a STATION constraint (role=WorkCenter) |
| `com.warehouse.wms.facility-layout.locationslot.LocationSlotDecommissioned` | `warehouse.facility.events` | facility-layout | `locationCode` -> decrements the same tally |

`process-path-management` is deliberately NOT consumed — its `ProcessPath`
carries `path_id`/`required_capabilities`/`eligibility`, never an ordered
step sequence, so there is nothing structural to sync.

## Consumer group id

Every Kafka consumer group id MUST come from an env var, never a hardcoded
string literal — `internal/architecture/fitness_test.go`'s
`TestKafkaConsumerGroupNeverHardcodedInline` enforces this.
