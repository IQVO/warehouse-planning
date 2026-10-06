---
id: domain-message-flow
title: Domain message flow
sidebar_label: Domain message flow
sidebar_position: 15
---

# Domain message flow

Following ddd-crew [Domain Message Flow Modelling](https://github.com/ddd-crew/domain-message-flow-modelling).
Each arrow is numbered and prefixed `cmd:` (command), `evt:` (event) or `qry:`
(query). Only real messages appear: REST routes from `NewRouter`, MCP tools from
`tools.go`, and CloudEvents types from the consumers and the encoder.

## 1. Capacity is known, a plan reveals a shortage, order-management is told

A shift plan is committed, the facility registers packing stations, a planner
declares the per-station standard, then creates and publishes a plan whose
demand exceeds the path capacity.

```mermaid
sequenceDiagram
  autonumber
  participant WFM as workforce-management
  participant FL as facility-layout
  actor Planner as Planner via console remote
  participant WP as warehouse-planning
  participant OM as order-management
  WFM->>WP: evt: ShiftPlanCommitted on warehouse.workforce.events
  Note over WP: LABOR constraint registered for PICK, REBIN, PACK at SIM1
  FL->>WP: evt: LocationSlotRegistered role WorkCenter, activities PACK
  Note over WP: station tally incremented, no capacity stored
  Planner->>WP: cmd: PUT /station-standards/SIM1/PACK
  Planner->>WP: cmd: POST /capacity-plans with assigned_demand
  Note over WP: path capacity composed at read time, plan saved as DRAFT
  WP-->>OM: evt: CapacityPlanCreated on warehouse.warehouse-planning.events
  Planner->>WP: cmd: POST /capacity-plans/{id}/publish
  WP-->>OM: evt: CapacityPlanPublished
  WP-->>OM: evt: CapacityShortageDetected, only because shortage is above 0
  WP-->>OM: evt: BottleneckDetected, ignored by order-management today
```

Source: `internal/adapters/inbound/kafka/labor_capacity_consumer.go`,
`storage_capacity_consumer.go`, `internal/adapters/inbound/http/process_capacity_handler.go`,
`internal/domain/capacityplan/capacity_plan.go`, `internal/adapters/outbound/kafka/encoder.go`;
order-management's `planned_capacity_consumer.go`.
Omits: the outbox relay hop between the commit and Kafka, the analytics copy of
every event, and the order-management consumer being opt-in.

## 2. Demand is defaulted from order-management's orders

The demand consumer is enabled (`DEMAND_CONSUMER_GROUP`, `DEMAND_SITE_ID=SIM1`).
A planner checks the expected demand and creates a plan without stating it.

```mermaid
sequenceDiagram
  autonumber
  participant OM as order-management
  actor Planner as Planner via console remote
  participant WP as warehouse-planning
  OM->>WP: evt: OrderAllocated on warehouse.order-management.events
  OM->>WP: evt: OrderPartiallyAllocated
  Note over WP: one order_demand row per order id, site SIM1, last writer wins
  Planner->>WP: qry: GET /demand?location=SIM1 and the window
  Note over Planner,WP: answer carries orders, released_lines, as_of
  Planner->>WP: cmd: POST /capacity-plans without assigned_demand
  alt orders expected in the window
    Note over WP: assigned_demand = orders, demand_source = orders, 201 DRAFT plan
  else no orders in the window
    Note over WP: 422 missing-assigned-demand, never a zero-demand plan
  end
```

Source: `internal/adapters/inbound/kafka/order_demand_consumer.go`,
`internal/application/usecases/expected_demand.go`,
`internal/application/usecases/create_capacity_plan.go` (`resolveDemand`),
`internal/domain/demand/order.go`, `docs/adr/0004-demand-ingestion-from-order-management.md`.
Omits: `OrderRepromised` and every other ignored type, and the events the
created plan emits (scenario 1).

## 3. The ops agent reads the capacity outlook over MCP

`warehouse-ops-agent` builds its daily brief from this context's read tools; it
never writes.

```mermaid
sequenceDiagram
  autonumber
  participant OPS as warehouse-ops-agent
  participant MCP as warehouse-planning MCP server
  Note over OPS: building the daily brief capacity outlook
  OPS->>MCP: qry: get_process_path_capacity id, location, window
  Note over OPS,MCP: answer carries normalized_rate, bottleneck_step, step_breakdown, warnings
```

Source: `internal/adapters/inbound/mcp/tools.go`, `cmd/mcp/main.go`;
warehouse-ops-agent's `internal/adapters/outbound/mcpclient/warehouse_planning.go`
and its ADR 0013.
Omits: the agent's other wired-but-unused read tools (`get_capacity_plan`,
`get_storage_capacity`, `list_station_standards`) and the agent's own LLM
steps.
