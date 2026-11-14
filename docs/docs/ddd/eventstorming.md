---
id: eventstorming
title: EventStorming
sidebar_label: EventStorming
sidebar_position: 16
---

# EventStorming (design level)

Notation from the ddd-crew
[EventStorming glossary and cheat sheet](https://github.com/ddd-crew/eventstorming-glossary-cheat-sheet).
Five processes, each read left to right. Hotspots are real, documented gaps
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
Omits: nothing; this is the key for the five diagrams below.

## 1. Capacity facts arrive from upstream

```mermaid
flowchart LR
  WFM[workforce-management]:::external --> E1[ShiftPlanCommitted]:::event
  E1 --> P1[Whenever a shift line is committed, register its LABOR constraint]:::policy
  P1 --> C1[RegisterProcessCapacityConstraint]:::command
  OP([Operator]):::actor --> C1
  C1 --> A1[ProcessCapacity]:::aggregate
  A1 --> R1[process_capacity read for path capacity]:::readmodel
  A1 --> E4[ProcessCapacityRegistered]:::event
  A1 --> E5[ProcessCapacityChanged]:::event
  E4 --> P3[Both events are written to the integration topic only, through the outbox]:::policy
  E5 --> P3
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
`declare_station_standard.go`, `internal/domain/processcapacity/*.go`
(`events.go`).
Omits: the processed-event claim and DLQ mechanics (see the
[sequence diagrams](/docs/ddd/sequence-diagrams)) and `RegisterProcessPath`,
which the operator also issues before planning. `ProcessCapacity` raises
`ProcessCapacityRegistered` when its first constraint is registered and
`ProcessCapacityChanged` when a registration moves the effective rate; a
registration that leaves the rate untouched raises nothing. Neither event has
a consumer yet (see [Domain events](/docs/ddd/domain-events)), and the
analytics fanout carries plan facts only (`apis/asyncapi.yaml`, ADR 0005).

## 2. Plan, publish, announce the shortage

```mermaid
flowchart LR
  PL([Planner]):::actor --> C1[CreateCapacityPlan]:::command
  R1[Path capacity composed at read time]:::readmodel --> C1
  R2[Expected demand]:::readmodel --> C1
  R5[Inbound appointment demand]:::readmodel --> C1
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
`publish_capacity_plan.go`, `inbound_demand.go`, `internal/domain/capacityplan/*.go`,
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
required `DEMAND_CONSUMER_GROUP` / `DEMAND_SITE_ID` configuration.

## 4. Inbound demand from dock-scheduling

```mermaid
flowchart LR
  DS[dock-scheduling]:::external --> E1[DockAppointmentBooked]:::event
  DS --> E2[DockAppointmentRescheduled]:::event
  DS --> E3[DockAppointmentCancelled]:::event
  DS --> E4[DockAppointmentNoShow]:::event
  E1 --> P1[Whenever an appointment event arrives, apply it if its version is newer, attributed to the site_id of the event]:::policy
  E2 --> P1
  E3 --> P1
  E4 --> P1
  P1 --> C1[RecordInboundAppointment]:::command
  C1 --> R1[inbound_appointments: one row per appointment, version guarded]:::readmodel
  OP([Operator]):::actor --> C2[DeclareInboundLabourStandard]:::command
  C2 --> R2[inbound_labour_standards per site and process path]:::readmodel
  PL([Planner]):::actor --> C3[CreateCapacityPlan]:::command
  R1 --> C3
  R2 --> C3
  H1[Appointments at a site with no declared inbound standard contribute no labour and the plan only warns]:::hotspot -.- R2

  classDef actor fill:#fff7a8,stroke:#b8a400,color:#000,font-size:11px
  classDef command fill:#4aa3df,stroke:#1f6fa3,color:#fff
  classDef aggregate fill:#f7d84a,stroke:#b39b12,color:#000
  classDef event fill:#f6a04d,stroke:#b8661c,color:#000
  classDef policy fill:#c39bd3,stroke:#7d4f91,color:#000
  classDef readmodel fill:#7dcea0,stroke:#2f8a57,color:#000
  classDef external fill:#f1948a,stroke:#a9483e,color:#000
  classDef hotspot fill:#e74c3c,stroke:#8e1f14,color:#fff
```

Source: `internal/adapters/inbound/kafka/dock_appointment_consumer.go`,
`internal/application/usecases/inbound_demand.go`
(`RecordInboundAppointment`, `DeclareInboundLabourStandard`,
`GetInboundDemand`), `create_capacity_plan.go` (`resolveInbound`), ADR 0017
and ADR 0020.
Omits: the processed-event claim and DLQ mechanics, and the retired source
(inbound-receiving's appointment events are no longer consumed, ADR 0020).

## 5. Observed capacity from fulfillment-execution

```mermaid
flowchart LR
  FX[fulfillment-execution]:::external --> E1[TaskCompleted]:::event
  E1 --> P1[Whenever a task completes, count it in the minute bucket of its process step at the configured site]:::policy
  P1 --> C1[RecordObservedCompletion]:::command
  C1 --> R1[observed_capacity: completions per site, step and minute]:::readmodel
  PL([Planner]):::actor --> Q1[GetCapacityPlanVariance]:::readmodel
  R1 --> Q1
  H1[Advisory only and every completion is attributed to the one configured site]:::hotspot -.- P1

  classDef actor fill:#fff7a8,stroke:#b8a400,color:#000,font-size:11px
  classDef command fill:#4aa3df,stroke:#1f6fa3,color:#fff
  classDef aggregate fill:#f7d84a,stroke:#b39b12,color:#000
  classDef event fill:#f6a04d,stroke:#b8661c,color:#000
  classDef policy fill:#c39bd3,stroke:#7d4f91,color:#000
  classDef readmodel fill:#7dcea0,stroke:#2f8a57,color:#000
  classDef external fill:#f1948a,stroke:#a9483e,color:#000
  classDef hotspot fill:#e74c3c,stroke:#8e1f14,color:#fff
```

Source: `internal/adapters/inbound/kafka/observed_capacity_consumer.go`,
`internal/application/usecases/observed_capacity.go`
(`RecordObservedCompletion`, `GetCapacityPlanVariance`), ADR 0017.
Omits: the processed-event claim and DLQ mechanics and the required
`OBSERVED_CAPACITY_CONSUMER_GROUP` / `DEMAND_SITE_ID` configuration. The
variance never changes a planned capacity.

## Sticky inventory

| Sticky | Kind | Code evidence |
| --- | --- | --- |
| Operator, Planner | Actor | REST callers (`warehouse-console` remote `web/src/api.ts`) and MCP clients |
| `RegisterProcessCapacityConstraint` | Command | `internal/application/usecases/register_process_capacity_constraint.go` |
| `DeclareStationStandard` | Command | `internal/application/usecases/declare_station_standard.go` |
| `CreateCapacityPlan` | Command | `internal/application/usecases/create_capacity_plan.go` |
| `PublishCapacityPlan` | Command | `internal/application/usecases/publish_capacity_plan.go` |
| `RecordOrderDemand` | Command | `internal/application/usecases/expected_demand.go` |
| `RecordInboundAppointment`, `DeclareInboundLabourStandard` | Command | `internal/application/usecases/inbound_demand.go` |
| `RecordObservedCompletion` | Command | `internal/application/usecases/observed_capacity.go` |
| `ProcessCapacity` | Aggregate | `internal/domain/processcapacity/process_capacity.go` |
| `CapacityPlan` | Aggregate | `internal/domain/capacityplan/capacity_plan.go` |
| `CapacityPlanCreated`, `CapacityPlanPublished`, `CapacityShortageDetected`, `BottleneckDetected` | Domain event (published) | `internal/domain/capacityplan/events.go`, `internal/adapters/outbound/kafka/encoder.go` |
| `ProcessCapacityRegistered`, `ProcessCapacityChanged` | Domain event (published, integration topic only) | `internal/domain/processcapacity/events.go`, `register_process_capacity_constraint.go` |
| `ShiftPlanCommitted` | Domain event (consumed) | `typeShiftPlanCommitted` in `labor_capacity_consumer.go` |
| `LocationSlotRegistered`, `LocationSlotDecommissioned` | Domain event (consumed) | `typeLocationSlotRegistered`, `typeLocationSlotDecommissioned` in `storage_capacity_consumer.go` |
| `OrderAllocated`, `OrderPartiallyAllocated` | Domain event (consumed) | `typeOrderAllocated`, `typeOrderPartiallyAllocated` in `order_demand_consumer.go` |
| `DockAppointmentBooked`, `DockAppointmentRescheduled`, `DockAppointmentCancelled`, `DockAppointmentNoShow` | Domain event (consumed) | `typeDockAppointment*` in `dock_appointment_consumer.go` |
| `TaskCompleted` | Domain event (consumed) | `typeTaskCompleted` in `observed_capacity_consumer.go` |
| Register LABOR on shift commit | Policy | `LaborCapacityConsumer.HandleMessage` |
| Adjust the tally on slot change | Policy | `StorageCapacityConsumer.handleRegistered` / `handleDecommissioned` |
| Record order demand | Policy | `OrderDemandConsumer.HandleMessage` |
| Apply a dock appointment event, version guarded | Policy | `DockAppointmentConsumer.HandleMessage` |
| Count a completed task in its minute bucket | Policy | `ObservedCapacityConsumer.HandleMessage` |
| Fan out to both topics through the outbox | Policy | `FanoutEncoder.Encode`, `enqueue` in `create_capacity_plan.go` |
| Path capacity, facility tally, StationStandard, expected demand, inbound demand, observed capacity and variance, `plan_facts`, reports | Read model | `GetProcessPathCapacity`, `location_slot_tally`, `station_standards`, `order_demand`, `inbound_appointments`, `inbound_labour_standards`, `observed_capacity`, `plan_facts`, `internal/analytics/report` |
| workforce-management, facility-layout, order-management, dock-scheduling, fulfillment-execution | External system | the five consumer files above (`labor_capacity_consumer.go`, `storage_capacity_consumer.go`, `order_demand_consumer.go`, `dock_appointment_consumer.go`, `observed_capacity_consumer.go`) and order-management's `planned_capacity_consumer.go` |
| Derived labor window, assumed UNIT/HOUR | Hotspot | ADR 0001 Addendum; `laborRateUnit` comment in `labor_capacity_consumer.go` |
| Stations without a standard only warn | Hotspot | `ComposeStepCapacity` warning, ADR 0002 |
| WorkloadProfile not stored | Hotspot | `GetProcessPathCapacityCommand` doc comment ("PHASE 2 SIMPLIFICATION") |
| BottleneckDetected without a consumer | Hotspot | order-management's `planned_capacity_consumer.go` ignores it |
| One site, no cancellation netting | Hotspot | ADR 0004, `cmd/api/runtime.go` |
| No inbound standard declared, appointments price at zero | Hotspot | `resolveInbound` in `create_capacity_plan.go` |
| Observed capacity is advisory and single-site | Hotspot | `observed_capacity_consumer.go`, ADR 0017 |
