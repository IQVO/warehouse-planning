# ADR 0002: Station capacity is composed at read time; storage positions are a read model

## Status

Accepted (2026-10-04). Supersedes the part of the ADR 0001 Addendum (2026-10-03,
"Confirmed consumed-event shapes", Storage/Station) that tallied
`facility-layout` slots into LOCATION/STATION `CapacityConstraint`s.
Decision 2's "registered at exactly `(P, L, W)`" is refined by ADR 0003: a
registered window applies when it COVERS `W`.

## Context

Phase 3 turned `facility-layout`'s `LocationSlotRegistered` /
`LocationSlotDecommissioned` stream into `ProcessCapacity` constraints, with
two workarounds recorded in `.claude/rules/integration-events.md`:

- storage positions under a sentinel `ProcessType` `STORAGE` with
  `Location = <zoneId>:<locationType>`;
- work-center stations as a STATION constraint in unit `LINE` on a
  `[epoch, epoch+100y)` "standing window", keyed by ZONE.

Checked against the live cluster data this never worked as a capacity model:

1. **They never combine with LABOR.** Labor is registered by the labor
   consumer on `Location = building_id` (e.g. `SIM1`) with a SHIFT window and
   unit UNIT/HOUR; the station constraint sat on a zone id (e.g.
   `SIM1-OPS-WC`) on the standing window. A `ProcessCapacity` is identified by
   `(ProcessType, Location, CapacityWindow)`, so the two were different
   aggregates and station counts never influenced
   `GET /process-paths/{id}/capacity` or a `CapacityPlan`.
2. **A station COUNT has no throughput.** Design doc section 19: 10 stations x
   180 packages/hour/station = 1,800 packages/hour. The count (11 stations)
   became a "11 LINE per hour" figure that no step could use, and the unit
   (LINE) cannot even be normalized to ORDER (design doc rule 8).
3. **Storage positions are not process throughput** (design doc sections
   14-17: the StorageCapacityPool idea). Forcing them onto
   `(ProcessType, Location, Window)` needed the sentinel type.

The real data: workforce's `building_id` is `SIM1`; facility zone ids are
`SIM1-OPS-WC` (WorkCenter, activity PACK, 11 stations) and `SIM1-STOR-AMB`
(Storage, locationType `SimShelf`, 24 slots).

Alternatives weighed:

- *Keep materializing, but re-key to the site and a SHIFT window.* Needs a
  constraint per labor window the consumer cannot know in advance, goes stale
  when a window or a standard changes, and still has no throughput per station.
- *Materialize `count x standard` whenever either changes.* Needs triggers on
  two unrelated inputs, leaves stale rows when one is deleted or declared late,
  and is order-dependent (the standard may arrive after the tally).
- *A site/zone mapping aggregate.* Extra operator-maintained state for a
  mapping the fleet's own code grammar already encodes.

## Decision

1. **The facility consumer becomes a pure tally maintainer.** It still claims
   the CloudEvents id and mutates `location_slot_tally` /
   `location_slot_registration` inside ONE `UnitOfWork` (the atomic
   at-least-once behaviour of ADR 0001's follow-up is unchanged), but it no
   longer registers any `ProcessCapacity`: no `STORAGE` sentinel, no standing
   window, no LINE. The labor consumer is unchanged.
2. **Composition happens at read time.** For a path step with process `P` at
   location `L` and window `W`, `GetProcessPathCapacity` (and therefore
   `CreateCapacityPlan`) takes as candidates (a) the constraints registered at
   exactly `(P, L, W)` and (b) a derived STATION constraint
   `stationCount(L, activity=P) x StationStandard(L, P)` when both a count > 0
   and a standard exist. Every candidate is normalized to ORDER/hour with the
   request's `WorkloadProfile` before the minimum is taken; the binding
   constraint type is reported (`step_breakdown`, plan `bottleneck_constraint`).
   The composition runs on transient values in a domain service
   (`ComposeStepCapacity` / `ComposeProcessPathCapacity`); the stored
   `ProcessCapacity` aggregate and its single-native-unit invariant are not
   weakened. Stations tallied with no standard declared: the step uses (a) only
   and a warning is returned -- no throughput is invented.
3. **`StationStandard` is an operator-declared planning parameter** keyed
   `(location, process_type)`, valued as a `CapacityRate` per ONE station in
   the process's natural unit (UNIT, PACKAGE or ORDER), declared with
   `PUT /station-standards/{location}/{process_type}` (MCP
   `declare_station_standard`) and stored in `station_standards`.
4. **Site = building id = first segment of the zone id.** A planning `location`
   is a site code; its zones are the tally zones whose id starts with
   `<location>-`. Migration `0005` also deletes the legacy derived
   ProcessCapacity rows (`process_type = 'STORAGE'`, and rows whose
   `window_start` is the 1970 standing-window start; constraints cascade) --
   derived data nothing read -- and adds the composition outcome columns to
   `capacity_plans`.
5. **Storage positions stay a read model**: `GET /storage-capacity?location=`
   (MCP `get_storage_capacity`) lists positions per `(zone, locationType)` and
   stations per `(zone, activity)` of a site.

## Consequences

Why read-time composition rather than materialized constraints: it is
**convergent** (the answer is a function of the current tally, standards and
constraints, whatever order they arrived in), leaves **no stale rows** to
repair when a slot is decommissioned or a standard replaced, and handles
**late declarations** -- declaring a standard after the stations were tallied
takes effect on the very next read. The cost is a few indexed reads per path
step (a tally sum and a standard lookup), which is small next to the rest of
the request.

Why throughput per station is an operator-declared parameter owned by this
context: no upstream publishes it (`facility-layout` knows that a work center
exists and which activities it supports, `workforce-management` knows heads and
rate per path, neither knows what a station can process per hour). It is a
planning assumption and is therefore explicit, visible (`GET /station-standards`)
and replaceable.

Why site = building id = zone-id first segment, with no mapping aggregate: it
is a documented convention of the fleet's own code grammar, not a guess.
facility-layout's `LocationCode` grammar is `Site-Area-Zone-...`, and its
`SiteCode` is upper-case alphanumeric with no dashes; workforce's `building_id`
is the same site code in the live data. The fail-safe is that a zone whose id
has no matching site simply does not contribute (never an error, never
mis-attributed to another site: `SIM1-` does not match `SIM10-`). The risk is
the convention drifting upstream; it is documented here, in
`integration-events.md`, and pinned by tests at every layer.

Explicit non-goals:

- **Storage positions are NOT process throughput.** They stay a read model
  (design doc sections 14-17, the StorageCapacityPool idea) and do not
  influence a path capacity or a plan.
- **No "consumed" figure.** Utilization of positions would need stock, and
  stock must never be read from inventory-storage (design doc rule 1; ADR 0001
  "Explicitly excluded").
- No per-station-instance modelling, no shift-dependent station availability, no
  events: the published CloudEvents types and payloads are unchanged
  (`bottleneck_constraint` and `warnings` are REST/MCP read-model fields only).

Operational note: migration `0005` deletes the legacy derived rows. They were
read by nothing, and the tally they were derived from is untouched, so the
facility consumer needs no replay.
