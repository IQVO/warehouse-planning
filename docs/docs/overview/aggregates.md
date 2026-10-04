---
id: aggregates
title: Aggregates
sidebar_position: 2
---

# Aggregates

Two aggregates carry the model. Domain code lives in
`internal/domain/processcapacity` and `internal/domain/capacityplan` and does
no I/O.

## ProcessCapacity

Package `internal/domain/processcapacity`.

- **Identity**: `(ProcessType, Location, CapacityWindow)`. The window is part of
  the identity and stays an *exact* key for the aggregate itself.
- **Constraints**: each is a `ConstraintType` plus a `CapacityRate`. The seven
  types are `LABOR`, `LOCATION`, `EQUIPMENT`, `STATION`, `CONVEYOR`, `BUFFER`
  and `REPLENISHMENT`. Adding a constraint of a type already present replaces
  it.
- **Invariants**: at least one constraint, and all constraints on one instance
  share the same native unit (a `UNIT` rate is never silently compared with a
  `PACKAGE` rate).
- **Effective rate**: `EffectiveRate()` is the minimum across the constraints
  and also reports which constraint type is binding.

Supporting value objects:

| Value object | Meaning |
| --- | --- |
| `CapacityRate` | quantity + native unit + period, for example `4000 UNIT / HOUR`; never compared across units without a `WorkloadProfile` |
| `CapacityWindow` | the `[start, end)` period a capacity is valid for; `Covers` implements [window coverage](/docs/overview/capacity-composition#window-coverage) |
| `WorkloadProfile` | per-warehouse conversion factors (units per order, packages per order); `NormalizeToOrderRate` converts a `UNIT` or `PACKAGE` rate to `ORDER` for the same period, passes `ORDER` through and rejects `LINE` |
| `StationStandard` | operator-declared throughput of ONE station of a process at a site, keyed `(location, process type)`; quantity must be positive and the unit `UNIT`, `PACKAGE` or `ORDER` |

## CapacityPlan

Package `internal/domain/capacityplan`.

A CapacityPlan ties the demand assigned to a warehouse location and planning
window to the path capacity available to serve it.

| Field | Notes |
| --- | --- |
| `id` | UUID string; natural key is `(WarehouseID, PlanningWindow)` |
| `WarehouseID`, `Location`, `PlanningWindow`, `ProcessPathID` | what is being evaluated |
| `AssignedDemand` | orders, `>= 0` |
| `PathCapacity` | computed at creation, ORDER per HOUR |
| `BottleneckStep` | the path step limiting end-to-end flow |
| `CapacityOverWindow` | `PathCapacity` x window hours |
| `Shortage` | `max(0, demand - capacityOverWindow)`; never negative, and demand exactly equal to the capacity is **not** a shortage |
| `Status` | `DRAFT` or `PUBLISHED` |
| `BottleneckConstraint`, `Warnings` | informational composition outcome (for example `STATION`); not carried by any published event |

Behaviour:

- `Create` takes the already computed path rate and bottleneck plus an explicit
  id and time; it records `CapacityPlanCreated`.
- `Publish` moves `DRAFT` to `PUBLISHED`. A second call returns
  `ErrAlreadyPublished` (never a silent double publish). It always records
  `CapacityPlanPublished`, and, only when `Shortage > 0`,
  `CapacityShortageDetected` and `BottleneckDetected`.
- Events are plain structs the aggregate accumulates; `PullEvents()` hands each
  over exactly once. `Rehydrate` rebuilds a stored plan without events.

Event order for a plan with a shortage: Created (at creation), then Published,
ShortageDetected, BottleneckDetected (at publish).

## Not aggregates

- **ProcessPathCapacity** is a domain *service* result
  (`ComposeProcessPathCapacity`), computed at read time and never stored: the
  minimum of the steps' effective capacities after normalization, with the
  bottleneck step and its binding constraint type.
- **ProcessPath** (`internal/domain/processpath`) is an operator-declared,
  ordered, non-empty step list stored as a read model.
- **Station counts and storage positions** are a tally of `facility-layout`
  events (`location_slot_tally`), exposed as a read model. A count has no
  throughput of its own.
