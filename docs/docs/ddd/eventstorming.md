---
id: eventstorming
title: EventStorming
sidebar_label: EventStorming
sidebar_position: 16
---

# EventStorming (design level)

Notation from the ddd-crew
[EventStorming glossary and cheat sheet](https://github.com/ddd-crew/eventstorming-glossary-cheat-sheet).
Three processes, each read left to right. Hotspots are real, documented gaps
(ADRs, code comments), not invented ones.

## Legend

```mermaid
flowchart LR
  A([Actor]):::actor
  C[Command]:::command
  G[Aggregate]:::aggregate
  E[Domain event]:::event
  P[Policy]:::policy
  R[Read model]:::readmodel
  X[External system]:::external
  H[Hotspot]:::hotspot
  classDef actor fill:#fff7a8,stroke:#b8a400,color:#000,font-size:11px
  classDef command fill:#4aa3df,stroke:#1f6fa3,color:#fff
  classDef aggregate fill:#f7d84a,stroke:#b39b12,color:#000
  classDef event fill:#f6a04d,stroke:#b8661c,color:#000
  classDef policy fill:#c39bd3,stroke:#7d4f91,color:#000
  classDef readmodel fill:#7dcea0,stroke:#2f8a57,color:#000
  classDef external fill:#f1948a,stroke:#a9483e,color:#000
  classDef hotspot fill:#e74c3c,stroke:#8e1f14,color:#fff
```

Source: ddd-crew cheat-sheet colours as specified for the fleet.
Omits: nothing; this is the key for the three diagrams below.

## 1. Capacity facts arrive from upstream

```mermaid
flowchart LR
  WFM[workforce-management]:::external --> E1[ShiftPlanCommitted]:::event
  E1 --> P1[Whenever a shift line is committed, register its LABOR constraint]:::policy
  P1 --> C1[RegisterProcessCapacityConstraint]:::command
  OP([Operator]):::actor --> C1
  C1 --> A1[ProcessCapacity]:::aggregate
  A1 --> R1[process_capacity read for path capacity]:::readmodel
  H1[Window derived from event time, planned_rate unit assumed UNIT per hour]:::hotspot -.- P1

  FL[facility-layout]:::external --> E2[LocationSlotRegistered]:::event
  FL --> E3[LocationSlotDecommissioned]:::event
  E2 --> P2[Whenever a slot changes, adjust the tally]:::policy
  E3 --> P2
  P2 --> R2[Facility tally: positions and stations per zone]:::readmodel

  OP --> C2[DeclareStationStandard]:::command
  C2 --> R3[StationStandard per site and process]:::readmodel
  H2[Stations tallied without a standard only warn]:::hotspot -.- R3

  classDef actor fill:#fff7a8,stroke:#b8a400,color:#000,font-size:11px
  classDef command fill:#4aa3df,stroke:#1f6fa3,color:#fff
  classDef aggregate fill:#f7d84a,stroke:#b39b12,color:#000
  classDef event fill:#f6a04d,stroke:#b8661c,color:#000
  classDef policy fill:#c39bd3,stroke:#7d4f91,color:#000
  classDef readmodel fill:#7dcea0,stroke:#2f8a57,color:#000
  classDef external fill:#f1948a,stroke:#a9483e,color:#000
  classDef hotspot fill:#e74c3c,stroke:#8e1f14,color:#fff
```

Source: `internal/adapters/inbound/kafka/labor_capacity_consumer.go`,
`storage_capacity_consumer.go`,
`internal/application/usecases/register_process_capacity_constraint.go`,
`declare_station_standard.go`, `internal/domain/processcapacity/*.go`.
Omits: the processed-event claim and DLQ mechanics (see the
[sequence diagrams](/docs/ddd/sequence-diagrams)) and `RegisterProcessPath`,
which the operator also issues before planning. No domain event is raised by
`ProcessCapacity` itself.

## 2. Plan, publish, announce the shortage

```mermaid
flowchart LR
  PL([Planner]):::actor --> C1[CreateCapacityPlan]:::command
  R1[Path capacity composed at read time]:::readmodel --> C1
  R2[Expected demand]:::readmodel --> C1
  C1 --> A1[CapacityPlan]:::aggregate
  A1 --> E1[CapacityPlanCreated]:::event
  PL --> C2[PublishCapacityPlan]:::command
  C2 --> A1
  A1 --> E2[CapacityPlanPublished]:::event
  A1 --> E3[CapacityShortageDetected]:::event
  A1 --> E4[BottleneckDetected]:::event
  E1 --> P1[Every plan event goes to both topics through the outbox]:::policy
  E2 --> P1
  E3 --> P1
  E4 --> P1
  P1 --> OM[order-management]:::external
  P1 --> R3[plan_facts analytics projection]:::readmodel
  R3 --> R4[Reports: bottleneck frequency, shortage trend, plan throughput, freshness]:::readmodel
  H1[BottleneckDetected has no consumer today]:::hotspot -.- E4
  H2[WorkloadProfile travels on every request, not stored]:::hotspot -.- C1

  classDef actor fill:#fff7a8,stroke:#b8a400,color:#000,font-size:11px
  classDef command fill:#4aa3df,stroke:#1f6fa3,color:#fff
  classDef aggregate fill:#f7d84a,stroke:#b39b12,color:#000
  classDef event fill:#f6a04d,stroke:#b8661c,color:#000
  classDef policy fill:#c39bd3,stroke:#7d4f91,color:#000
  classDef readmodel fill:#7dcea0,stroke:#2f8a57,color:#000
  classDef external fill:#f1948a,stroke:#a9483e,color:#000
  classDef hotspot fill:#e74c3c,stroke:#8e1f14,color:#fff
```

Source: `internal/application/usecases/create_capacity_plan.go`,
`publish_capacity_plan.go`, `internal/domain/capacityplan/*.go`,
`internal/adapters/outbound/kafka/analytics_encoder.go` (`FanoutEncoder`),
`internal/adapters/outbound/analyticsstore/postgres_projection.go`,
`internal/adapters/inbound/http/reports_handler.go`.
Omits: the rejection paths (`ErrAlreadyPublished`, `ErrMissingAssignedDemand`,
`ErrMissingStepCapacity`). `CapacityShortageDetected` and `BottleneckDetected`
are raised only when `shortage > 0`.

## 3. Expected demand from order-management

```mermaid
flowchart LR
  OM[order-management]:::external --> E1[OrderAllocated]:::event
  OM --> E2[OrderPartiallyAllocated]:::event
  E1 --> P1[Whenever an order is allocated, record its promise cutoff for the configured site]:::policy
  E2 --> P1
  P1 --> C1[RecordOrderDemand]:::command
  C1 --> R1[order_demand: one row per order, last writer wins]:::readmodel
  PL([Planner]):::actor --> Q1[GetExpectedDemand]:::readmodel
  R1 --> Q1
  H1[All orders attributed to one site, cancellations not netted]:::hotspot -.- P1

  classDef actor fill:#fff7a8,stroke:#b8a400,color:#000,font-size:11px
  classDef command fill:#4aa3df,stroke:#1f6fa3,color:#fff
  classDef aggregate fill:#f7d84a,stroke:#b39b12,color:#000
  classDef event fill:#f6a04d,stroke:#b8661c,color:#000
  classDef policy fill:#c39bd3,stroke:#7d4f91,color:#000
  classDef readmodel fill:#7dcea0,stroke:#2f8a57,color:#000
  classDef external fill:#f1948a,stroke:#a9483e,color:#000
  classDef hotspot fill:#e74c3c,stroke:#8e1f14,color:#fff
```

Source: `internal/adapters/inbound/kafka/order_demand_consumer.go`,
`internal/application/usecases/expected_demand.go`,
`internal/domain/demand/order.go`, ADR 0004.
Omits: `OrderRepromised` (deliberately ignored: no new cutoff instant) and the
opt-in switch `DEMAND_CONSUMER_GROUP`.

## Sticky inventory

| Sticky | Kind | Code evidence |
| --- | --- | --- |
| Operator, Planner | Actor | REST callers (`warehouse-console` remote `web/src/api.ts`) and MCP clients |
| `RegisterProcessCapacityConstraint` | Command | `internal/application/usecases/register_process_capacity_constraint.go` |
| `DeclareStationStandard` | Command | `internal/application/usecases/declare_station_standard.go` |
| `CreateCapacityPlan` | Command | `internal/application/usecases/create_capacity_plan.go` |
| `PublishCapacityPlan` | Command | `internal/application/usecases/publish_capacity_plan.go` |
| `RecordOrderDemand` | Command | `internal/application/usecases/expected_demand.go` |
| `ProcessCapacity` | Aggregate | `internal/domain/processcapacity/process_capacity.go` |
| `CapacityPlan` | Aggregate | `internal/domain/capacityplan/capacity_plan.go` |
| `CapacityPlanCreated`, `CapacityPlanPublished`, `CapacityShortageDetected`, `BottleneckDetected` | Domain event (published) | `internal/domain/capacityplan/events.go`, `internal/adapters/outbound/kafka/encoder.go` |
| `ShiftPlanCommitted` | Domain event (consumed) | `typeShiftPlanCommitted` in `labor_capacity_consumer.go` |
| `LocationSlotRegistered`, `LocationSlotDecommissioned` | Domain event (consumed) | `typeLocationSlotRegistered`, `typeLocationSlotDecommissioned` in `storage_capacity_consumer.go` |
| `OrderAllocated`, `OrderPartiallyAllocated` | Domain event (consumed) | `typeOrderAllocated`, `typeOrderPartiallyAllocated` in `order_demand_consumer.go` |
| Register LABOR on shift commit | Policy | `LaborCapacityConsumer.HandleMessage` |
| Adjust the tally on slot change | Policy | `StorageCapacityConsumer.handleRegistered` / `handleDecommissioned` |
| Record order demand | Policy | `OrderDemandConsumer.HandleMessage` |
| Fan out to both topics through the outbox | Policy | `FanoutEncoder.Encode`, `enqueue` in `create_capacity_plan.go` |
| Path capacity, facility tally, StationStandard, expected demand, `plan_facts`, reports | Read model | `GetProcessPathCapacity`, `location_slot_tally`, `station_standards`, `order_demand`, `plan_facts`, `internal/analytics/report` |
| workforce-management, facility-layout, order-management | External system | the three consumer files above and order-management's `planned_capacity_consumer.go` |
| Derived labor window, assumed UNIT/HOUR | Hotspot | ADR 0001 Addendum; `laborRateUnit` comment in `labor_capacity_consumer.go` |
| Stations without a standard only warn | Hotspot | `ComposeStepCapacity` warning, ADR 0002 |
| WorkloadProfile not stored | Hotspot | `GetProcessPathCapacityCommand` doc comment ("PHASE 2 SIMPLIFICATION") |
| BottleneckDetected without a consumer | Hotspot | order-management's `planned_capacity_consumer.go` ignores it |
| One site, no cancellation netting | Hotspot | ADR 0004, `cmd/api/demand.go` |
