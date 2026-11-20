---
id: aggregates
title: Aggregates
sidebar_position: 2
---

# Aggregates

Four aggregates carry the model: two on the capacity side
(`internal/domain/processcapacity`, `internal/domain/capacityplan`) and, since
ADR 0023, two on the requirement side (`internal/domain/workloadforecast`,
`internal/domain/labourplan`). Domain code does no I/O.

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
| `DemandSource` | `request` (stated by the caller) or `orders` (defaulted from the expected-demand read model, ADR 0004); informational, no published event carries it |
| `Status` | `DRAFT` or `PUBLISHED` |
| `BottleneckConstraint`, `Warnings` | informational composition outcome (for example `STATION`); no integration event carries them, and only the analytics copy of `CapacityPlanPublished` carries the constraint (`binding_constraint`, ADR 0005) |
| `LabourRequirement` | per step of the path (ADR 0022): `standard_seconds` from the labor-performance copy in force at the window start, `tasks`, `required_labour_hours = tasks x standard_seconds / 3600`, `required_average_heads` over the window, `known` and a `reason` (`no-labor-standard`, `no-task-factor`) when unknown; computed at creation, stored, on no event |

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

## WorkloadForecast

Package `internal/domain/workloadforecast` (ADR 0023 decision 1; it was
wes-work-planning's charge forecast).

- **Identity**: `(site_id, path_id)`; `site_id` is a canonical facility-layout
  site code.
- **Content**: CPT buckets `{cpt, quantity}`, quantity a non-negative whole
  number of units; at least one bucket. A receipt replaces the buckets as a
  whole.
- **Versioning**: version 1 on the first receipt, + 1 per receipt that changed
  the buckets. Equal buckets in the same order change nothing: no event, same
  version, same `received_at`.
- **Event**: `WorkloadForecastReceived` (`site_id`, `path_id`,
  `total_quantity`, `bucket_count`, `received_at`, `version`).
- **Query**: `QuantityDueIn(from, to)` sums the buckets whose `cpt` is in
  `[from, to)` and reports how many there were; zero buckets is no data.

## LabourPlan

Package `internal/domain/labourplan` (ADR 0023 decisions 2 and 3; it was
wes-work-planning's path plan).

| Field | Notes |
| --- | --- |
| `site_id`, `shift_id`, `path_id` | the identity; `site_id` and `shift_id` matched exactly as sent, `path_id` trimmed and upper-cased |
| `planned_heads` | the requirement; `0..2147483647` and never above `installed_stations` |
| `rate_units_per_hour`, `hours` | above zero; `rate x heads x hours` must be finite |
| `installed_stations` | caller-supplied, stored as sent |
| `shift_start` | optional; needed to derive heads |
| `heads_source` | `request` (sent) or `forecast` (derived: `ceil(units due in [shift_start, shift_start + hours) / (rate x hours))`) |
| `version` | 1, then + 1 per commit that changed the plan |

Behaviour:

- `New` and `Revise` record `LabourPlanCommitted` (every field above); a spec
  equal to the stored one changes nothing.
- The station warning is not part of the aggregate: the use case computes it
  against the facility tally on every read, and only when the tally knows at
  least one but fewer stations of the activity named like the path.
- The **drift** lives in the same package but is not plan state:
  `CompareHeads(planned, committed)` gives `committed - planned`;
  `ShouldRaise(last, hasLast)` is true when it is non-zero and differs from the
  last drift recorded for the key. The heads workforce-management committed
  (`Commitment`) are a read model, not an aggregate; neither drift trigger
  changes the plan or the commitment.

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
- **Expected demand** (`internal/domain/demand`, ADR 0004) is a read model of
  `order-management` orders (`order_demand`): one `demand.Order` per order id,
  last writer wins on the CloudEvents `time` (`Order.Supersedes`), counted in a
  window `[start, end)` by its promise cutoff (`Order.CountsIn`). Zero orders is
  *no data*, never zero demand (`Summary.HasData`).
- **Labour standards** (`internal/domain/laborstandard`, ADR 0022) are a local
  copy of labor-performance's facts (`labor_standards`, one row per
  `standard_id`); `Resolve` picks the one in force for a site, task type and
  window start at read time.
- **Committed heads** (`labourplan.Commitment`, `labour_plan_commitments`) are a
  read model of workforce-management's `ShiftPlanCommitted`, plus the last
  drift recorded for the key.

The [Aggregate Design Canvas](/docs/ddd/aggregate-design-canvas) covers all four
aggregates in the ddd-crew format.
