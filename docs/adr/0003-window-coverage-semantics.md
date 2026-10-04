# ADR 0003: A registered capacity window applies when it COVERS the planning window

## Status

Accepted (2026-10-04). Refines ADR 0002 decision 2, which looked constraints
up "at exactly `(P, L, W)`", and the exact-window statements in the ADR 0001
Addendum.

## Context

Path capacity (`GET /process-paths/{id}/capacity`, MCP
`get_process_path_capacity`) and `POST /capacity-plans` took the requested
window `W = [start, end)` and looked each step's `ProcessCapacity` up under
EXACTLY `(process, location, W)`. That was fine for hand-registered fixtures
and could never work for the live data.

The labor consumer registers one `ProcessCapacity` per fanned-out
`ShiftPlanCommitted` line with window `[event.time, event.time + planned_hours)`
(`integration-events.md`; there is no shift-start field upstream). Checked
against the live cluster at location `SIM1`, the rows of ONE commit share the
same start (the event time) and differ in their end, because each path line has
its own `planned_hours`:

| step  | start (shared)  | end      |
|-------|-----------------|----------|
| PICK  | event time `S`  | `S + 32h` |
| REBIN | event time `S`  | `S + 8h`  |
| PACK  | event time `S`  | `S + 24h` |

(The unit-test fixtures replicate this shape with `S = 2026-10-01T22:26:55Z`.)
There is no single `(start, end)` pair registered for all three steps of the
`PICK -> REBIN -> PACK` path, so an exact-key lookup could not resolve a real
path at any window: every request returned `422 missing-step-capacity`.

Alternatives weighed:

- *Change the labor window derivation so all lines of a commit end together*
  (for example, the longest `planned_hours`). Rejected: it invents a window the
  producer never stated and silently stretches a short line's capacity beyond
  the hours planned for it.
- *Interval overlap* (any intersecting window applies). Rejected: a constraint
  planned for 08:00-16:00 would "apply" to a 15:00-23:00 request that is half
  outside it, overstating capacity.
- *Containment of the registered window in the request (the reverse).* Rejected:
  the wrong direction for the same reason.

## Decision

1. **Coverage.** A constraint registered for window `C` applies to a planning
   window `W` when `C` COVERS `W`: `C.start <= W.start AND C.end >= W.end`.
   A window covers itself, so everything that resolved before still resolves.
   `CapacityWindow.Covers` is the single definition; the Postgres query
   restates it as `window_start <= $3 AND window_end >= $4`.
2. **Repository.** `ports.ProcessCapacityRepository.FindCovering(process,
   location, start, end)` returns every covering aggregate, newest first
   (`ORDER BY window_start DESC, window_end ASC`, the order
   `processcapacity.SortNewestFirst` defines), an empty slice when none covers,
   and `ErrInvalidWindow` for an inverted or zero-length request.
   `FindByProcessLocationWindow` stays, with exact-key semantics.
3. **Newest wins, per constraint TYPE.** Several aggregates can cover one
   window (a re-plan registered later, a longer older shift). Per constraint
   type, `ComposeStepCapacity` takes the constraint from the covering aggregate
   with the LATEST start (tie: the narrower window, i.e. the earlier end); an
   older or wider aggregate's constraint of the SAME type is shadowed. A
   constraint of another type in the older aggregate still applies (a newer
   LABOR constraint does not hide an older EQUIPMENT one). Every candidate is
   normalized to ORDER/hour before the minimum is taken, so covering aggregates
   with different native units compose, and the derived STATION constraint of
   ADR 0002 still participates.
4. **The plan keeps the requested window.** `CapacityPlan.PlanningWindow` is
   `W`, and `capacity_over_window = path_capacity x hours(W)` and the shortage
   come from the REQUESTED window length, never from a constraint's own window.
5. **Missing coverage is explicit.** A step with no covering aggregate and no
   STATION constraint is still `ErrMissingStepCapacity` (`422
   missing-step-capacity`); the use case wraps it with the location and the
   requested window and the message names the step.

## Consequences

- The live `SIM1` path resolves for every window inside the INTERSECTION of the
  step windows (`[S, S + 8h)` there, bound by REBIN); a window reaching past
  one step's end is not covered by it and is rejected, naming that step. A
  step whose labor window is short is a real limit: capacity is not assumed
  beyond the hours planned.
- The labor window derivation `[event time, event time + planned_hours)` is
  KEPT as is (`integration-events.md`). Nothing about the consumer, its events
  or stored rows changes.
- **Behavior change:** `GET /process-paths/{id}/capacity` with `window_end` not
  after `window_start` now returns `400 invalid-capacity-window`
  (`ErrInvalidWindow`). Before, an inverted window was only a miss on the exact
  key (`422`). `POST /capacity-plans` already rejected it.
- **No migration `0006`.** The primary key `(process_type, location,
  window_start, window_end)` has `window_start` as its third column, so the
  range predicate on `window_start` after the equality prefix is served by the
  existing index; one location holds one row per labor line, which is a few
  rows per request. An index is a candidate if measurements ever disagree.
- No data change, no event change (published CloudEvents are untouched), no
  REST/MCP shape change: only which rows a lookup finds.

## Explicit non-goals

- **`GET`/`POST /process-capacities` and MCP `get_effective_process_capacity` /
  `register_process_capacity_constraint` keep EXACT-key semantics.** Those
  address one aggregate by its identity `(process, location, window)`:
  registering twice at one key upserts, and reading it returns that aggregate.
  Coverage is a rule for CAPACITY COMPOSITION (path capacity and plans), not
  for identity.
- No overlap or partial-window pro-rating, no stitching of adjacent windows
  into a covering one, no change to the `CapacityWindow` invariant (`end` strictly
  after `start`).
