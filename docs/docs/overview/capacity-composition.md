---
id: capacity-composition
title: Capacity composition
sidebar_position: 3
---

# Capacity composition

How the capacity of a process-path step is resolved. This page summarises
[ADR 0002](/docs/adr/0002-station-capacity-composition) and
[ADR 0003](/docs/adr/0003-window-coverage-semantics); the ADRs are the
authority.

## Which path is resolved (ADR 0025)

A `path_id` resolves to an ACTIVE process flow first (its steps are the flow's
stages' `process_type` values in order, kept in the event-fed `process_flows`
copy), else to the locally declared `ProcessPath` (`POST /process-paths`).
A RETIRED flow does not resolve. Every path read says which one it used:
`source: flow` or `source: local`.

## Window coverage

A window `C` **covers** a planning window `W` when `C.start <= W.start` and
`C.end >= W.end` (`CapacityWindow.Covers`; a window covers itself). A
registered constraint applies to a requested window by coverage, not by exact
match.

Why: the labor consumer registers one ProcessCapacity per fanned-out
`ShiftPlanCommitted` line with window `[event.time, event.time + planned_hours)`.
Lines of one commit share the start and differ in their end (the ADR records
live data at `SIM1`: PICK +32h, REBIN +8h, PACK +24h), so an exact-window
lookup could never match a planning window.

Resolution among covering aggregates: **per constraint type**, the constraint
from the aggregate with the *latest window start* wins (tie: the narrower
window, i.e. the earlier end). An older or wider covering aggregate's
constraint of the *same type* is shadowed; constraints of other types still
apply. The aggregate's own identity stays an exact key, and the
register/get-effective endpoints (`/process-capacities`) keep exact window keys.

## Station capacity

A station **count** tallied from `facility-layout` has no throughput. The
operator declares the throughput of one station (a `StationStandard`, for
example 180 PACKAGE per hour for PACK at `SIM1`), and the service composes
`count x standard` at **read time**:

```text
station capacity = stationCount(location, activity = process) x StationStandard(location, process)
```

- A planning `location` is a site/building code such as `SIM1`; the zones of a
  site are the tally zones whose id starts with `<location>-` (for example
  `SIM1-OPS-WC`).
- The derived STATION candidate exists only when both a station count above
  zero and a standard exist.
- Stations tallied but no standard declared: the step uses its registered
  constraints only and a **warning** is carried. A throughput is never
  invented.
- Nothing derived is stored, so the result converges, has no stale rows and
  handles late declarations.

## Step composition (`ComposeStepCapacity`)

For a path step with process `P` at location `L` and window `W`:

1. Candidates: the covering constraints (one per constraint type, as above)
   plus the derived STATION constraint, when present.
2. Every candidate is normalized to ORDER per hour with the request's
   `WorkloadProfile` **before** comparing (units per hour and packages per hour
   are not comparable raw).
3. The step's effective rate is the minimum, and its binding constraint type
   (`LABOR`, `STATION`, ...) is reported. Ties go to the earliest candidate:
   registered constraints in precedence order (newest covering aggregate
   first, registration order within one aggregate), then STATION.
4. No candidate at all: `ErrMissingStepCapacity` (REST `422 missing-step-capacity`).

The path capacity is the minimum over the steps; the step that produces it is
the bottleneck step. `GET /process-paths/{id}/capacity` returns
`normalized_rate`, `normalized_unit` (`ORDER`), `bottleneck_step`, a per-step
`step_breakdown` and `warnings`.

## Worked example

Using the figures from the OpenAPI examples and the REST rules: a path
`pick-rebin-pack` limited to 1000 ORDER per hour by REBIN, a planning window of
8 hours and 12000 assigned orders give a `capacity_over_window` of 8000 and a
`shortage` of 4000. Publishing that plan raises `CapacityPlanPublished`,
`CapacityShortageDetected` and `BottleneckDetected` (bottleneck step `REBIN`).

## Inbound labour demand and observed capacity

Two inputs sit **next to** the ORDER capacity, never inside it ([ADR 0017](/docs/adr/0017-inbound-labour-demand-and-observed-capacity-feedback)):

- **Inbound labour demand.** `dock-scheduling`'s INBOUND dock appointments (a local read model attributed to the events' `site_id`; an appointment counts in a window when its door window *starts* in `[start, end)`, a reschedule moves it, a cancellation or no-show removes it) are priced with an operator-declared *inbound labour standard* per site and process path: `minutes_per_appointment` plus an optional `minutes_per_asn`. A plan stores `inbound_appointments`, `inbound_labour_minutes` and `inbound_standard_missing` and exposes `inbound_average_heads` (minutes over the window length). They are not added to `assigned_demand`: orders and labour minutes are different units. No standard declared means no demand and a visible `inbound standard missing` warning, never a guessed number.
- **Observed capacity.** `fulfillment-execution`'s `TaskCompleted` events are aggregated per site, process step and minute. `GET /capacity-plans/{id}/variance?tasks_per_order=` compares the tasks completed for the plan's bottleneck step in its window with the plan's ORDER capacity over that window. It is **advisory**: nothing here changes a planned capacity or republishes a plan (automatic adjustment is a later ADR). No completions means `has_data: false`, not zero throughput; without `tasks_per_order` the comparison is `comparable: false`.

## Storage positions are a read model

Storage positions per `(zone, location type)` and stations per
`(zone, activity)` are exposed by `GET /storage-capacity?location=`. They are
not process throughput and have no "consumed" figure: stock is never read from
`inventory-storage`.

## Requirement versus capacity

Everything above is **capacity**: what the warehouse can process. Since
[ADR 0022](/docs/adr/0022-labour-standards-from-labor-performance) and
[ADR 0023](/docs/adr/0023-workload-forecast-and-labour-plan-move-here) the
context also states the **requirement**: how much labour the demand needs. The
two are kept apart on purpose.

| | Capacity | Requirement |
| --- | --- | --- |
| Question | can the path process the assigned demand in the window? | how many labour hours and heads does the demand need? |
| Unit | ORDER per hour, shortage in orders | labour hours per step; heads per path and shift |
| Built from | registered constraints (LABOR from workforce-management's committed heads, STATION from the tally x a station standard, ...) | labor-performance's engineered standards (per step), the workload forecast (per shift) |
| Where | `ProcessCapacity`, `CapacityPlan` (shortage, bottleneck) | `CapacityPlan.labour_requirement`, `LabourPlan` |
| Published | `CapacityPlan*`, `ProcessCapacity*` events | `LabourPlanCommitted` (and `LabourPlanDriftDetected`) |

The LABOR **constraint** is supply (the heads workforce-management committed,
times their rate); the labour **requirement** is demand. Neither changes the
other: the labour requirement never enters the ORDER shortage or the
bottleneck, and the drift between the labour plan and the commitment is
surfaced, never corrected.

### Labour requirement per step (capacity plans)

When a capacity plan is created, each step of its path gets a labour
requirement (`labour_requirement` on `POST /capacity-plans` and
`GET /capacity-plans/{id}`, stored with the plan):

```text
tasks                  = released lines of the counted orders     (PICK, demand_source = orders)
                       = assigned_demand x tasks_per_order[step]  (every other case)
required_labour_hours  = tasks x standard_seconds / 3600
required_average_heads = required_labour_hours / window hours
```

- `standard_seconds` is the labor-performance standard in force at the window
  start for the step's task type: a step whose process type is `PICK`, `PACK`
  or `SLAM` names its task type; a site-scoped standard beats a fleet-wide
  one, then the latest `effective_from`, then the latest `as_of`.
- A step without a standard is `known: false`, `reason: no-labor-standard`; a
  step without a task factor is `known: false`, `reason: no-task-factor`.
  Never a guessed number.
- The requirement is computed once, at creation: a later standard revision
  never rewrites a stored plan.

### Heads per shift (labour plans)

A labour plan states the heads one path needs for one shift. Omitted heads are
derived from the stored workload forecast:

```text
planned_heads = ceil(units with cpt in [shift_start, shift_start + hours) / (rate_units_per_hour x hours))
```

Example from `features/labour_plan.feature`: buckets of 500 units at 07:00Z,
300 at 13:59Z and 999 at 14:00Z, a shift starting 06:00Z for 8 hours at 10
units per hour: the 14:00Z bucket is outside `[06:00, 14:00)`, so 800 units
are due and `ceil(800 / 80) = 10` heads. Zero buckets in the shift is no data:
`422 missing-planned-heads`, never zero heads. `planned_heads` never exceeds the
caller's `installed_stations`; the facility tally only adds a warning when it
knows at least one but fewer stations of the activity named like the path.
