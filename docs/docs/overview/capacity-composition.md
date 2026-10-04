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
   (`LABOR`, `STATION`, ...) is reported. Ties go to the earliest candidate
   (registered constraints in registration order, then STATION).
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

## Storage positions are a read model

Storage positions per `(zone, location type)` and stations per
`(zone, activity)` are exposed by `GET /storage-capacity?location=`. They are
not process throughput and have no "consumed" figure: stock is never read from
`inventory-storage`.
