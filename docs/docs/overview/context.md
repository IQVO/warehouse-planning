---
id: context
title: Bounded context
sidebar_position: 1
---

# Bounded context

## Purpose

`warehouse-planning` evaluates whether a warehouse can process the demand
assigned to it. It composes the capacity constraints that sibling contexts and
operators make known, normalizes them to one comparable flow unit, and reports
the effective end-to-end capacity of a process path, the bottleneck step, and
the shortage of a planning window.

The decision to create it as a separate context (rather than extending an
existing service) is [ADR 0001](/docs/adr/0001-warehouse-planning-bounded-context):
`workforce-management` explicitly stops at the path boundary, and no other
context combined labor, station and location constraints.

## Context map

```mermaid
flowchart LR
  WFM[workforce-management] -- "ShiftPlanCommitted" --> WP
  FL[facility-layout] -- "LocationSlotRegistered / Decommissioned" --> WP
  WP((warehouse-planning))
  WP -- "CapacityPlan* events" --> OM[order-management and other consumers]
  WP -- "REST / MCP read queries" --> OPS[warehouse-ops-agent, warehouse-console]
```

ADR 0001 records the intended relationships (Customer/Supplier unless noted).
Only the rows marked *implemented* below exist in this repository today.

| Relationship | Direction | Status |
| --- | --- | --- |
| `workforce-management` to `warehouse-planning` | labor via Kafka (`ShiftPlanCommitted`) | implemented (consumer) |
| `facility-layout` to `warehouse-planning` | storage positions and stations via Kafka (`LocationSlotRegistered`, `LocationSlotDecommissioned`) | implemented (consumer, tally only) |
| `process-path-management` to `warehouse-planning` | `path_id` as a loose human cross-reference | deliberately **not** consumed (ADR 0001 Addendum) |
| `warehouse-planning` to other contexts | `CapacityPlanCreated`, `CapacityPlanPublished`, `CapacityShortageDetected`, `BottleneckDetected` | implemented (producer) |
| `warehouse-planning` to ops tooling | read-only capacity queries over REST and MCP | implemented |
| `fulfillment-execution`, `wes-work-planning`, `order-management`, `network-fulfillment` | observed capacity feedback and demand ingestion | planned in ADR 0001, **not** implemented; assigned demand currently travels in the `POST /capacity-plans` body |

## No live cross-context lookup

The service never makes a synchronous REST or MCP call to a sibling bounded
context to answer a capacity question. Capacity-relevant facts are ingested
as published Kafka events and kept as local read models, so a capacity
decision stays available and fast even when an upstream context is degraded.
`inventory-storage` stock levels are explicitly excluded: stock is not
capacity.

## Runtime surface

- `cmd/api` serves REST on `HTTP_ADDR` (default `:8080`), runs the Kafka
  consumers and the outbox relay, and applies the idempotent migrations on
  start.
- `cmd/mcp` serves an MCP server (10 tools) over Streamable HTTP on `MCP_ADDR`
  (default `:8090`, mounted at `/` and `/mcp`, with `GET /healthz`). It uses
  the same use cases and repositories as REST, never dials Kafka and never
  starts the outbox relay: the relay in `cmd/api` drains the outbox rows that
  MCP create and publish calls insert.
- `cmd/planning-projector` (admin `:8091`, `/healthz` `/readyz`) is the analytics
  read side's only writer: it consumes `warehouse.warehouse-planning.analytics`
  and projects into the separate analytical database (ADR 0005).
  `cmd/planning-reports` (`:8092`, `/healthz`) serves the three read-only
  `/reports/...` endpoints from it.
- Without `DATABASE_URL` both binaries fall back to in-memory repositories.
- A Helm chart (`charts/warehouse-planning`) deploys the `api` component, and
  optionally the `mcp` and the analytics components (off by default).

Not delivered yet (tracked deferrals): metrics for the analytics processes (they
expose no `/metrics`).
