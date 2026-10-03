# ADR 0001: warehouse-planning as a new Core bounded context

## Status

Accepted (2026-10-03)

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
| `process-path-management` -> `warehouse-planning` | path topology (steps, capability requirements) | Conformist — never forks the upstream structure |
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

- This context owns `ProcessCapacity`, `CapacityPlan` and a read-only copy
  of `ProcessPath` — it does not own labor scheduling, storage slotting,
  or path topology authoring, all of which remain in their existing
  contexts.
- Upstream event contracts (`workforce-management`, `facility-layout`)
  must be confirmed exact before the event-ingestion phase starts; if
  either does not yet publish a usable event, that is a cross-repo
  prerequisite, tracked separately rather than guessed at.
- `order-management` needs a follow-up change (separate PR, separate repo)
  to actually consume `WarehouseCapacityPublished`/`CapacityShortageDetected`
  — out of scope for this repo's own delivery.
