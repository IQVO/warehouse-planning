---
id: events
title: Events (AsyncAPI)
sidebar_position: 99
---

# Events

Summary of `apis/asyncapi.yaml` (AsyncAPI 2.6.0, version 0.4.0). The file is the
authority for payload fields.

## Envelope

Every message, published or consumed, is a **CloudEvents 1.0** event in
structured content mode with the Kafka header
`content-type: application/cloudevents+json; charset=UTF-8`. There is no flat
envelope and no toggle.

| Attribute | Value |
| --- | --- |
| `specversion` | `1.0` |
| `id` | UUID v4 minted once per domain event and persisted with the outbox row; consumers dedupe on it |
| `source` | `/warehouse/warehouse-planning` |
| `type` | `com.warehouse.wes.warehouse-planning.<entity>.<EventName>` (here `<entity>` is `capacityplan`) |
| `subject` | the aggregate instance id (the capacity plan id); also the Kafka key |
| `time` | domain occurred-at, UTC |
| `datacontenttype` | `application/json` |
| `dataschema` | `urn:warehouse:warehouse-planning:events:<EventName>:v1` |

A breaking payload change gets a new `.v2` type and dataschema; an existing one
is never mutated.

## Published

Topic `warehouse.warehouse-planning.events`, written by the outbox relay.

| Type suffix (after `com.warehouse.wes.warehouse-planning.capacityplan.`) | Raised | Payload (`data`) |
| --- | --- | --- |
| `CapacityPlanCreated` | `POST /capacity-plans` | `plan_id`, `warehouse_id`, `location`, `path_id`, `window_start`, `window_end`, `assigned_demand`, `path_capacity`, `capacity_over_window`, `shortage`, `bottleneck_step`, `status` (always `DRAFT`) |
| `CapacityPlanPublished` | plan publication | the same fields except `status`, plus `published_at` |
| `CapacityShortageDetected` | publication, only when `shortage > 0` | `plan_id`, `warehouse_id`, `location`, `path_id`, `window_start`, `window_end`, `assigned_demand`, `capacity_over_window`, `shortage`, `bottleneck_step` |
| `BottleneckDetected` | publication, only when `shortage > 0` | `plan_id`, `warehouse_id`, `location`, `path_id`, `window_start`, `window_end`, `bottleneck_step`, `path_capacity` |

Quantities are orders; `path_capacity` is ORDER per hour; times are RFC 3339 UTC.
`ProcessCapacityRegistered` and `ProcessCapacityChanged` are **not** published,
and there is no analytics stream
(`warehouse.warehouse-planning.analytics`) yet.

## Consumed

| Type | Topic | Producer | Consumer group env var |
| --- | --- | --- | --- |
| `com.warehouse.wes.workforce-management.shiftplan.ShiftPlanCommitted` | `warehouse.workforce.events` | workforce-management | `LABOR_CAPACITY_CONSUMER_GROUP` |
| `com.warehouse.wms.facility-layout.locationslot.LocationSlotRegistered` | `warehouse.facility.events` | facility-layout | `STORAGE_CAPACITY_CONSUMER_GROUP` |
| `com.warehouse.wms.facility-layout.locationslot.LocationSlotDecommissioned` | `warehouse.facility.events` | facility-layout | `STORAGE_CAPACITY_CONSUMER_GROUP` |
| `com.warehouse.wes.order-management.order.OrderAllocated` | `warehouse.order-management.events` | order-management | `DEMAND_CONSUMER_GROUP` (unset = not consumed) |
| `com.warehouse.wes.order-management.order.OrderPartiallyAllocated` | `warehouse.order-management.events` | order-management | `DEMAND_CONSUMER_GROUP` (unset = not consumed) |

The two order-management types feed the expected-demand read model
(`GET /demand`, MCP `get_expected_demand`, and the default of `assigned_demand`
on `POST /capacity-plans`); every other type on that topic, `OrderRepromised`
included, is ignored. See `docs/adr/0004-demand-ingestion-from-order-management.md`.

What each consumed event changes is described on
[Upstream contracts](/docs/ecosystem/upstream-contracts).

## Example

A `CapacityShortageDetected` event from the spec (12000 orders assigned, 8000
orders of capacity, bound by `REBIN`):

```json
{
  "specversion": "1.0",
  "id": "3b2a1c0d-9e8f-4d7c-b6a5-4f3e2d1c0b9a",
  "source": "/warehouse/warehouse-planning",
  "type": "com.warehouse.wes.warehouse-planning.capacityplan.CapacityShortageDetected",
  "subject": "0b7a4c1e-5d52-4f0e-9a39-6c1f2f3a8b10",
  "time": "2026-10-04T21:45:10Z",
  "datacontenttype": "application/json",
  "dataschema": "urn:warehouse:warehouse-planning:events:CapacityShortageDetected:v1",
  "data": {
    "plan_id": "0b7a4c1e-5d52-4f0e-9a39-6c1f2f3a8b10",
    "warehouse_id": "WH-1",
    "location": "PATH-ZONE-A",
    "path_id": "pick-rebin-pack",
    "window_start": "2026-10-05T08:00:00Z",
    "window_end": "2026-10-05T16:00:00Z",
    "assigned_demand": 12000,
    "capacity_over_window": 8000,
    "shortage": 4000,
    "bottleneck_step": "REBIN"
  }
}
```

The full spec is at
[`apis/asyncapi.yaml`](https://raw.githubusercontent.com/IQVO/warehouse-planning/main/apis/asyncapi.yaml).
