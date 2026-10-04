# CLAUDE.md — warehouse-planning

This repo is `warehouse-planning`, a Core bounded context in the
`warehouse-systems` fleet (GitHub org `IQVO`). It answers: *can this
warehouse process the demand assigned to it, given its current labor,
location, equipment, station, conveyor and buffer constraints?* — a
capability no other fleet service currently provides (Inventory Management
answers "what do we have," this context answers "how much work can we
perform").

Scaffolded from `warehouse-harness-template: v2`
(`IQVO/warehouse-harness-template`). See `HARNESS.md` in this repo for the
full sensor manifest (what runs when, and why) and `SPEC.md` for the
template's own notes (kept for reference; this file is the real guide).

## Domain model

See `.claude/rules/domain-model.md` — ubiquitous language, aggregates,
domain events, use cases. Read it before writing any domain code.

## REST API

See `.claude/rules/rest-api.md`.

## MCP

`cmd/mcp` is this context's MCP server (ADR-0008, additive inbound adapter
in `internal/adapters/inbound/mcp/`): official Go SDK, Streamable HTTP only
on :8090 at `/` and `/mcp`, open `/healthz`. It calls the same use cases
and Postgres repos as REST, has no auth, never starts the outbox relay and
never dials Kafka (the `cmd/api` relay drains the outbox rows its create
and publish tools insert). Tool list and arguments: `.claude/rules/mcp.md`.

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
  via the ONE helper package, `internal/adapters/kafka/cloudevents/`.
  Transport stays `segmentio/kafka-go`.
- Every produced message carries the Kafka header
  `content-type: application/cloudevents+json; charset=UTF-8`
  (`cloudevents.ContentTypeHeader()`).
- Required attributes: `specversion=1.0`; `id` (UUID v4 minted ONCE per
  domain event and persisted with the outbox row); `source=/warehouse/warehouse-planning`;
  `type`; `subject` (aggregate instance id, never empty); `time` (domain
  occurred-at, UTC); `datacontenttype=application/json`;
  `dataschema=urn:warehouse:warehouse-planning:<events|analytics>:<EventName>:v<N>`.
- `type` = `com.warehouse.wes.warehouse-planning.<entity>.<EventName>`
  (this context is `wes`-tier per the fleet's subdomain table: `wms` is
  reserved for `facility-layout`/`inventory-storage` only). Breaking
  payload change => new `.v2` type + new dataschema version, never mutate.
- Consumers decode with `cloudevents.Decode` (validates), dispatch on the
  FULL `type` string, ignore unknown types, dedupe on `id`, and DLQ/skip —
  never crash, never block the partition — anything that fails CloudEvents
  validation.

Full standard, subdomain table and the fleet's cross-service type
catalogue: `warehouse-docs` `docs/strategic-design/event-standard-cloudevents.md`.

### Published types

Topic `warehouse.warehouse-planning.events`, key = capacity plan id,
`subject` = capacity plan id, written through the transactional outbox
(`outbox_events`, relay in `cmd/api`, `EVENT_PUBLISHER=kafka|log`):

- `com.warehouse.wes.warehouse-planning.capacityplan.CapacityPlanCreated`
- `com.warehouse.wes.warehouse-planning.capacityplan.CapacityPlanPublished`
- `com.warehouse.wes.warehouse-planning.capacityplan.CapacityShortageDetected`
- `com.warehouse.wes.warehouse-planning.capacityplan.BottleneckDetected`

Payload shapes: `apis/asyncapi.yaml`. There is no analytics stream yet.
The `<entity>` segment is lowercase with no separators (fleet standard).

### Consumed types

Confirmed against the producers' own `apis/asyncapi.yaml` (see ADR 0001
Addendum and `.claude/rules/integration-events.md`):

- `com.warehouse.wes.workforce-management.shiftplan.ShiftPlanCommitted`
  on `warehouse.workforce.events`: the labor consumer registers a LABOR
  constraint on `Location=building_id` for `[event time, + planned_hours)`.
- `com.warehouse.wms.facility-layout.locationslot.LocationSlotRegistered`
  and `...LocationSlotDecommissioned` on `warehouse.facility.events`
  (storage/station tallies).

`process-path-management` is deliberately not consumed: its `ProcessPath`
has no physical step sequence, so this context owns its own `ProcessPath`.

Delivery is at-least-once: each message is handled in one unit of work
(claim + write), the offset is committed only after success, and
transient failures retry the same message with backoff. The facility
consumer only maintains the storage/station tally (`location_slot_tally`);
it registers no ProcessCapacity. Station capacity is composed at READ time
(station count x operator-declared `StationStandard`, ADR 0002), and step
capacity is resolved by window COVERAGE, newest registration wins per
constraint type (ADR 0003). A planning `location` is the site/building code
(the first segment of a facility zone id, e.g. `SIM1` for zone
`SIM1-OPS-WC`).

## Consumer group id

Every Kafka consumer group id MUST come from an env var, never a string
literal — `TestKafkaConsumerGroupNeverHardcodedInline` enforces this.

## No auth

This service has no auth on REST or MCP, matching the fleet-wide
2026-09-11 static-bearer-auth revert. Do not add auth middleware —
`TestNoAuthMiddlewareReintroduced` fails CI if it's reintroduced.

## Cross-context integration rule

No live REST/MCP calls to sibling bounded contexts at request time.
Capacity-relevant facts from `workforce-management` and `facility-layout`
are consumed as published Kafka events and kept as local read models — the
same rule already established in
`process-path-management`/`labor-performance`, generalized here because a
capacity decision must stay available and fast even if an upstream
context is degraded. See `docs/adr/0001-warehouse-planning-bounded-context.md`
(placement, context map, upstream contracts), `docs/adr/0002-station-capacity-composition.md`
and `docs/adr/0003-window-coverage-semantics.md`.

## Harness version

`harness-template: v2`.
