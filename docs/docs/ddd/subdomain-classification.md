---
id: subdomain-classification
title: Subdomain classification
sidebar_label: Subdomain classification
sidebar_position: 3
---

# Subdomain classification

Domain-Driven Design splits a domain into **Core**, **Supporting** and
**Generic** subdomains by how much competitive advantage each one gives, not by
size or difficulty. This page states where `warehouse-planning` sits, why, and
how its direct neighbours are classified.

## This context: Core, `wes` tier

| Dimension | Value | Evidence |
| --- | --- | --- |
| Classification | **Core** | [ADR 0001](/docs/adr/0001-warehouse-planning-bounded-context) introduces the context as a Core Domain; the fleet table in warehouse-docs (`docs/strategic-design/subdomain-classification.md`) agrees |
| Tier (CloudEvents subdomain segment) | `wes` | Every published type is `com.warehouse.wes.warehouse-planning.capacityplan.<EventName>` (`internal/domain/capacityplan/events.go`, `internal/adapters/outbound/kafka/encoder_test.go`) |
| Business model | Risk reduction / engagement creator | [Bounded context canvas](/docs/ddd/bounded-context-canvas): it earns no revenue itself; it protects promised delivery by detecting a capacity shortage before the window starts |
| Evolution | Custom-built, recently out of genesis | Created 2026-10-03; its core rules were changed by ADRs 0002, 0003 and 0004 |
| Chart position | x = 0.74 (model complexity), y = 0.84 (business differentiation) | [Core domain chart](/docs/ddd/core-domain-chart) |

The tier and the classification are separate axes: `wes` says which tier the
context publishes under (warehouse execution, as opposed to the `wms`
warehouse-management tier); Core says how much the business differentiates on
it.

## Why Core

- **No other context answers the question.** "Can this warehouse process the
  demand assigned to it?" needs a normalized, cross-process effective capacity
  and a forward-looking shortage. `workforce-management` stops at the path
  boundary, `facility-layout` knows structure but not throughput, and
  `inventory-storage` knows stock, which is not capacity (ADR 0001).
- **The business acts on the answer.** `order-management` consumes
  `CapacityPlanCreated`, `CapacityPlanPublished` and `CapacityShortageDetected`
  (see [Upstream contracts](/docs/ecosystem/upstream-contracts)), so a shortage
  detected here changes what the fleet promises.
- **The rules are specific to this fleet.** Read-time station composition
  (`count x StationStandard`, [ADR 0002](/docs/adr/0002-station-capacity-composition)),
  window coverage with newest-start-wins per constraint type
  ([ADR 0003](/docs/adr/0003-window-coverage-semantics)), normalization through
  a per-request `WorkloadProfile`, and demand defaulting from an order read
  model where zero orders means *no data*
  ([ADR 0004](/docs/adr/0004-demand-ingestion-from-order-management)). None of
  this is an off-the-shelf module.

### Why not higher, and what would move it

The computations are deterministic minimums over small sets, with no
optimisation or forecasting model, which keeps the chart point off the far
right. Several inputs are still documented assumptions (the labor window is
derived from the event time, `planned_rate` is assumed to be `UNIT/HOUR`,
demand is attributed to the one configured `DEMAND_SITE_ID`). The context
should stay custom-built while those are replaced by real upstream facts.

### Slices are not classified separately

No ADR or DDD page in this repo classifies a slice of the context on its own.
The analytics read side (`cmd/planning-projector`, `cmd/planning-reports`,
[ADR 0005](/docs/adr/0005-analytics-read-side)) and the MCP server
(`cmd/mcp`, [ADR 0008](/docs/adr/0008-mcp-server-adoption)) are delivery
mechanisms around the Core model, not separately classified subdomains.

## Neighbour classifications

Copied from the fleet table in warehouse-docs
(`docs/strategic-design/subdomain-classification.md` on `main`), not
re-derived here. The relationship column is this context's edge, from
[Upstream contracts](/docs/ecosystem/upstream-contracts) and the code.

| Neighbour | Fleet classification | Tier | Relationship to warehouse-planning |
| --- | --- | --- | --- |
| `workforce-management` | Supporting | `wes` | Upstream: `ShiftPlanCommitted` on `warehouse.workforce.events` becomes a `LABOR` constraint |
| `facility-layout` | Generic | `wms` | Upstream: `LocationSlotRegistered` / `LocationSlotDecommissioned` on `warehouse.facility.events` maintain the storage and station tally |
| `order-management` | Generic/Supporting | `wes` | Both: upstream for demand (`OrderAllocated`, `OrderPartiallyAllocated`, consumer off unless `DEMAND_CONSUMER_GROUP` is set); downstream consumer of the capacity-plan events |
| `warehouse-ops-agent` | Supporting | `wes` | Downstream: reads this context through four read-only MCP tools |
| `process-path-management` | Generic | `wes` | No edge: its catalogue is deliberately not consumed; the two share the `path_id` string only as a loose cross-reference |
| `inventory-storage` | Core | `wms` | No edge: named only to contrast stock with capacity |

The pattern is typical for a Core context: it depends on Supporting and Generic
upstreams for raw facts (labor plans, location slots, orders) and adds the
differentiating composition on top. See the
[Context map](/docs/ddd/context-map) for the integration patterns per edge.
