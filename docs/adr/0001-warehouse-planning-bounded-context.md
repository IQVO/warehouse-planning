# ADR 0001: warehouse-planning as a new Core bounded context

## Status

Accepted (2026-10-03). Amended (2026-10-03) — see Addendum.

## Context

The fleet's bounded contexts (`inventory-storage`, `facility-layout`,
`workforce-management`, `process-path-management`, `fulfillment-execution`,
`wes-work-planning`, `order-management`, `network-fulfillment`,
`labor-performance`) each know a piece of what constrains warehouse
throughput — labor, storage, path topology, execution-time flow — but none
of them answers "can this warehouse process the demand assigned to it."
`workforce-management` explicitly stops at the path boundary. A DDD
reference study of Warehouse Capacity Planning
(`/Users/claudioed/.hermes/plans/2026-10-03_185721-warehouse-planning-bc-plan.md`
on the maintainer's machine) identified this as a genuine missing
bounded context, not something to bolt onto an existing service.

## Decision

Introduce `warehouse-planning` as a new **Core Domain** bounded context,
sitting in the `wes` tier of the fleet's CloudEvents subdomain taxonomy
(`wms` is reserved for `facility-layout`/`inventory-storage` only — see
`warehouse-systems-fleet-ops` skill's subdomain table).

### Context map (Customer/Supplier unless noted)

| Relationship | Direction | Pattern |
|---|---|---|
| `process-path-management` -> `warehouse-planning` | `path_id` identity + `required_capabilities` only (see Addendum — NOT a step sequence) | Conformist on identity, never forks the upstream structure |
| `workforce-management` -> `warehouse-planning` | available labor-hours per pool/window | Published Language via Kafka |
| `facility-layout` -> `warehouse-planning` | location/zone structural capacity | Published Language via Kafka |
| `fulfillment-execution` / `wes-work-planning` -> `warehouse-planning` | observed/current effective capacity (feedback, later phase) | Published Language via Kafka |
| `order-management` / `network-fulfillment` -> `warehouse-planning` | assigned demand for a planning window | Published Language via Kafka (final shape to be confirmed before the demand-ingestion phase) |
| `warehouse-planning` -> `order-management` | `WarehouseCapacityPublished` / `CapacityShortageDetected` | Published Language, consumed by order-management in a later, separate change |
| `warehouse-planning` -> `warehouse-ops-agent`, `warehouse-console` | read-only capacity queries | Open Host Service via REST/MCP |

**Explicitly excluded**: `inventory-storage` stock levels are never
consumed directly. Storage *capacity* comes from `facility-layout`
structural data, never from inventory quantities (stock is not capacity).

### No live cross-context lookup

`warehouse-planning` never makes a synchronous REST/MCP call to a sibling
bounded context to answer a capacity question. All cross-context facts
used in a capacity computation are ingested as published CloudEvents and
kept as local, declarative read models. This generalizes the rule already
adopted by `process-path-management` and `labor-performance`: a capacity
decision must stay available and fast even when an upstream context is
degraded or unreachable.

## Consequences

- This context owns `ProcessCapacity`, `CapacityPlan` and `ProcessPath`
  (the physical process-step sequence — see Addendum, this is now a
  LOCALLY OWNED declaration, not a Conformist copy) — it does not own
  labor scheduling, storage slotting, or path capability/eligibility
  authoring, all of which remain in their existing contexts.
- `order-management` needs a follow-up change (separate PR, separate repo)
  to actually consume `WarehouseCapacityPublished`/`CapacityShortageDetected`
  — out of scope for this repo's own delivery.

## Addendum (2026-10-03): upstream event contract spike (Task 0.4)

Before building Phase 3's event consumers, the exact upstream contracts
were read from each producer's own `apis/asyncapi.yaml` on
`origin/develop` rather than guessed. Two findings changed the original
Decision:

### 1. `process-path-management` does not publish a process-step sequence

Its `ProcessPath` aggregate (`ProcessPathCreated`/`Updated`/`Deactivated`
on `warehouse.process-path-management.events`,
`com.warehouse.wes.process-path-management.processpath.<EventName>`)
carries `path_id`, `match_prefix`, `direct`, `required_capabilities`,
`destination_location_role`, `cycle_time_p95`, `eligibility` — routing and
capability-matching metadata, never an ordered list of physical process
steps (Pick -> Rebin -> Pack). There is nowhere else in the fleet that
publishes that sequence either.

**Correction**: `warehouse-planning`'s `ProcessPath` (the ordered
`[]ProcessType` used by `ComputeProcessPathCapacity`) is a **locally
owned, operator-declared concept** (via `POST /process-paths`, already
shipped in Phase 2), not a Conformist copy of
`process-path-management`'s aggregate. The two contexts share the same
`path_id` string as a loose cross-reference (so a human/operator can line
up "PICK the capability-routing path" with "PICK the physical step" by
eye), but `warehouse-planning` never consumes
`process-path-management`'s Kafka events to populate its own `ProcessPath`
steps. Phase 3 therefore does NOT include a process-path-management
consumer.

### 2. Confirmed consumed-event shapes for Phase 3

- **Labor**: `workforce-management` publishes
  `com.warehouse.wes.workforce-management.shiftplan.ShiftPlanCommitted` on
  `warehouse.workforce.events`. Fan-out: one message per `PathPlan` line
  within a committed `ShiftPlan`. `data`: `building_id`, `shift_id`,
  `path_id`, `planned_heads`, `planned_rate`, `planned_hours`. No explicit
  window start/end is carried — Phase 3 derives the `CapacityWindow` as
  `[event time, event time + planned_hours]`, a documented assumption to
  revisit if `workforce-management` later exposes a real shift-start
  timestamp distinct from the event's own `time`. LABOR constraint rate =
  `planned_heads * planned_rate` in whatever native unit `planned_rate`
  is in for that `path_id` (units/hour per the fleet's established
  convention).
- **Storage/Station**: `facility-layout` has no dedicated capacity event.
  `LocationSlotRegistered`/`LocationSlotDecommissioned` on
  `warehouse.facility.events`
  (`com.warehouse.wms.facility-layout.locationslot.<EventName>`) carry
  `locationCode`, `zoneId`, `locationType`, `role` (default `Storage`),
  and — only when `role=WorkCenter` — an `activities` array (`Pack`,
  `Sort`, `QC`, `VAS`, `Deconsolidate`, `Receive`, `Kit`). Phase 3 tallies
  these itself, mirroring inventory-storage's existing
  location-classification cache pattern (per-process-unique consumer
  group, FirstOffset replay):
  - `role=Storage` slots, counted per `(zoneId, locationType)`, feed a
    LOCATION `CapacityConstraint` (position count as a capacity proxy).
  - `role=WorkCenter` slots whose `activities` includes a given process
    name, counted per zone, feed a STATION `CapacityConstraint` for that
    process (the doc's "10 packing stations" concept).
  `LocationSlotDecommissioned` decrements the same tally.
