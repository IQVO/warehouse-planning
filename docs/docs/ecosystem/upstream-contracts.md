---
id: upstream-contracts
title: Upstream contracts
sidebar_position: 1
---

# Upstream contracts

The consumed event shapes were confirmed against each producer's own
`apis/asyncapi.yaml` (see the Addendum of
[ADR 0001](/docs/adr/0001-warehouse-planning-bounded-context)) rather than
guessed. Messages are matched on the **full** CloudEvents `type`; unknown types
are ignored.

## workforce-management

| | |
| --- | --- |
| Type | `com.warehouse.wes.workforce-management.shiftplan.ShiftPlanCommitted` |
| Topic | `warehouse.workforce.events` |
| Fan-out | one message per PathPlan line of a committed ShiftPlan |
| Fields used | `building_id`, `path_id`, `planned_heads`, `planned_rate`, `planned_hours` |
| Effect | a `LABOR` constraint on the ProcessCapacity of ProcessType = upper-case(`path_id`) at Location = `building_id` |
| Rate | `planned_heads * planned_rate`, registered as `UNIT/HOUR` (a documented default; the native unit of `planned_rate` is not specified upstream) |
| Window | `[event.time, event.time + planned_hours)` (a documented assumption: no real shift-start field exists upstream yet) |

This is the **only** thing the consumers register as a ProcessCapacity
constraint.

## facility-layout

| | |
| --- | --- |
| Types | `com.warehouse.wms.facility-layout.locationslot.LocationSlotRegistered` and `...LocationSlotDecommissioned` |
| Topic | `warehouse.facility.events` |
| Fields used | `locationCode`, `zoneId`, `locationType`, `role` (default `Storage`), `activities` (present only when `role=WorkCenter`) |
| Effect | maintains a tally: storage positions per `(zoneId, locationType)` for `role=Storage`, stations per zone and activity for `role=WorkCenter`; decommissioning decrements the same tally |

The facility consumer is a pure tally maintainer: it registers no
ProcessCapacity, no sentinel process type and no fake unit. Station counts
become capacity only at read time, composed with an operator-declared
StationStandard
([capacity composition](/docs/overview/capacity-composition)).

## process-path-management: deliberately not consumed

Its `ProcessPath` carries `path_id`, `required_capabilities` and eligibility
metadata but no ordered physical step sequence, so there is nothing structural
to sync. This context owns its own `ProcessPath`, declared via
`POST /process-paths`; the two share the `path_id` string only as a loose
cross-reference.

## Downstream

The four CapacityPlan events are published on `warehouse.warehouse-planning.events`
(see the [event catalogue](/docs/api-reference/events)). ADR 0001 notes that
`order-management` needs a separate follow-up change to consume them.
