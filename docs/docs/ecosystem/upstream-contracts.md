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

## order-management

[ADR 0004](/docs/adr/0004-demand-ingestion-from-order-management). The
consumer is **off** unless `DEMAND_CONSUMER_GROUP` is set; it then requires
`DEMAND_SITE_ID`.

| | |
| --- | --- |
| Types | `com.warehouse.wes.order-management.order.OrderAllocated` and `com.warehouse.wes.order-management.order.OrderPartiallyAllocated` |
| Topic | `warehouse.order-management.events` |
| Fields used | `order_id` (must equal the CloudEvents `subject`), `promise_date`, `len(lines)`, and the CloudEvents `time` |
| Effect | upserts one `order_demand` row per order id (last writer wins on `time`); every order is attributed to the one configured site `DEMAND_SITE_ID`, because the events carry no fulfillment site |
| Ignored | `OrderRepromised` (no new cutoff instant) and every other type on the topic |

Demand is therefore counted in orders (no units) and not netted for
cancellations. It is read by `GET /demand`, the `get_expected_demand` MCP tool
and `POST /capacity-plans` when `assigned_demand` is omitted.

## Dead-letter topics

A message whose handling keeps failing transiently is retried up to 5 times and
then published to `<topic>.dlq` (`warehouse.workforce.events.dlq`,
`warehouse.facility.events.dlq`, `warehouse.order-management.events.dlq`) with
`x-dlq-*` headers ([ADR 0007](/docs/adr/0007-outbox-and-resilient-consumers)).
Deterministic problems (not a CloudEvent, unknown type, malformed payload,
missing fields) are logged and skipped, never dead-lettered.

## process-path-management: deliberately not consumed

Its `ProcessPath` carries `path_id`, `required_capabilities` and eligibility
metadata but no ordered physical step sequence, so there is nothing structural
to sync. This context owns its own `ProcessPath`, declared via
`POST /process-paths`; the two share the `path_id` string only as a loose
cross-reference.

## Downstream

The four CapacityPlan events are published on `warehouse.warehouse-planning.events`
(see the [event catalogue](/docs/api-reference/events)). The follow-up change
ADR 0001 anticipated has landed in `order-management` (its
`internal/adapters/inbound/kafka/planned_capacity_consumer.go`), which acts on
`CapacityPlanCreated`, `CapacityPlanPublished` and `CapacityShortageDetected`
and ignores `BottleneckDetected`. `warehouse-ops-agent` reads this context
through four read-only MCP tools (`get_process_path_capacity`,
`get_capacity_plan`, `get_storage_capacity`, `list_station_standards`). The
whole slice is drawn on the [Context map](/docs/ddd/context-map).
