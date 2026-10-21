---
id: domain-events
title: Domain events
sidebar_label: Domain events
sidebar_position: 21
---

# Domain events

Every event this context publishes or consumes. All are CloudEvents 1.0 in
**structured** content mode (`internal/adapters/kafka/cloudevents`: the whole
event is the JSON message value, Kafka header
`content-type: application/cloudevents+json; charset=UTF-8`). Payload fields
come from `apis/asyncapi.yaml` and the encoder / consumer structs; the
[event catalogue](/docs/api-reference/events) summarises the AsyncAPI file.

## Published

Raised by the `CapacityPlan` aggregate (`internal/domain/capacityplan/events.go`),
encoded by `internal/adapters/outbound/kafka/encoder.go` and inserted into
`outbox_events` inside the use case's unit of work; the relay in `cmd/api`
publishes them. Common attributes: `source=/warehouse/warehouse-planning`,
`subject` = Kafka key = **partition key** = the raising aggregate's id
(`kafkago.Hash` balancer) — the capacity plan id, or
`<processType>/<location>/<windowStart>/<windowEnd>` for a ProcessCapacity,
`time` = domain occurred-at,
`dataschema=urn:warehouse:warehouse-planning:events:<EventName>:v1`.

| Full CloudEvents type | Topic | Producer use case | Payload (`data`) | Known consumers |
| --- | --- | --- | --- | --- |
| `com.warehouse.wes.warehouse-planning.capacityplan.CapacityPlanCreated` | `warehouse.warehouse-planning.events` | `CreateCapacityPlan` | `plan_id`, `warehouse_id`, `location`, `path_id`, `window_start`, `window_end`, `assigned_demand`, `path_capacity`, `capacity_over_window`, `shortage`, `bottleneck_step`, `status` (always `DRAFT`) | `order-management` (`planned_capacity_consumer.go`, opt-in) |
| `com.warehouse.wes.warehouse-planning.capacityplan.CapacityPlanPublished` | `warehouse.warehouse-planning.events` | `PublishCapacityPlan`, always | the Created fields without `status`, plus `published_at` and (additively, ADR 0012) `site_id` — a facility-layout Site `site_code`, omitted for plans stored before migration 0008 | `order-management` |
| `com.warehouse.wes.warehouse-planning.capacityplan.CapacityShortageDetected` | `warehouse.warehouse-planning.events` | `PublishCapacityPlan`, only when `shortage > 0` | `plan_id`, `warehouse_id`, `location`, `path_id`, `window_start`, `window_end`, `assigned_demand`, `capacity_over_window`, `shortage`, `bottleneck_step` | `order-management` |
| `com.warehouse.wes.warehouse-planning.capacityplan.BottleneckDetected` | `warehouse.warehouse-planning.events` | `PublishCapacityPlan`, only when `shortage > 0` | `plan_id`, `warehouse_id`, `location`, `path_id`, `window_start`, `window_end`, `bottleneck_step`, `path_capacity` | none (`order-management` ignores it) |
| `com.warehouse.wes.warehouse-planning.processcapacity.ProcessCapacityRegistered` | `warehouse.warehouse-planning.events` | `RegisterProcessCapacityConstraint`, when the FIRST constraint of a (process, location, window) is registered | `process_type`, `location`, `window_start`, `window_end`, `constraint_type`, `quantity`, `unit`, `period_seconds` | none yet |
| `com.warehouse.wes.warehouse-planning.processcapacity.ProcessCapacityChanged` | `warehouse.warehouse-planning.events` | `RegisterProcessCapacityConstraint`, when a registration moves the effective rate (value, binding, or both) | `process_type`, `location`, `window_start`, `window_end`, `constraint_type`, `previous_quantity`, `previous_unit`, `previous_period_seconds`, `previous_binding_constraint`, `effective_quantity`, `effective_unit`, `effective_period_seconds`, `effective_binding_constraint` | none yet |

Units: `assigned_demand`, `capacity_over_window` and `shortage` are orders;
`path_capacity` is ORDER per hour; times are RFC 3339 UTC.

Event order on a plan with a shortage: Created (at creation), then Published,
ShortageDetected, BottleneckDetected (at publish, in that order in the outbox).
On a ProcessCapacity: `ProcessCapacityRegistered` for the first constraint,
then one `ProcessCapacityChanged` per registration that moves the effective
rate; a registration the effective rate does not notice (a non-binding
constraint, or a re-registration with the rate it already had) raises
nothing. Both `POST /process-capacities` and the labor-capacity consumer
(`ShiftPlanCommitted` upserting a LABOR constraint) drive the same use case
and raise the same events, outboxed in the same unit of work as the save.

### Analytics copies (ADR 0005)

`FanoutEncoder` writes every occurrence a second time, with the **same `type`
and `id`**, to `warehouse.warehouse-planning.analytics` with
`dataschema=urn:warehouse:warehouse-planning:analytics:<EventName>:v1`. The
payloads are identical except that `CapacityPlanPublished` adds
`binding_constraint` (the constraint type binding the bottleneck step, `""` when
unknown). The only consumer is this context's own `cmd/planning-projector`
(group `ANALYTICS_CONSUMER_GROUP`, default `warehouse-planning-analytics`); its
dead-letter topic is `warehouse.warehouse-planning.analytics.dlq`.

The ProcessCapacity events get **no analytics copy**: the analytics stream is
plan facts only (ADR 0005); `AnalyticsEncoder` refuses one handed to it
rather than silently dropping it.

## Consumed

Consumers decode with `cloudevents.Decode`, dispatch on the full `type`, ignore
every other type, and dedupe on the CloudEvents `id` in `processed_events`
inside the same unit of work as the effect.

| Full CloudEvents type | Topic | Producer | Fields used | Effect | Consumer, group env, DLQ |
| --- | --- | --- | --- | --- | --- |
| `com.warehouse.wes.workforce-management.shiftplan.ShiftPlanCommitted` | `warehouse.workforce.events` | `workforce-management` | `building_id`, `path_id`, `planned_heads`, `planned_rate`, `planned_hours` (`shift_id` is mirrored but unused), CloudEvents `time` | upserts a LABOR constraint `planned_heads x planned_rate` UNIT/HOUR on `ProcessCapacity(upper(path_id), building_id, [time, time + planned_hours))` | `LaborCapacityConsumer`, `LABOR_CAPACITY_CONSUMER_GROUP`, `warehouse.workforce.events.dlq` |
| `com.warehouse.wms.facility-layout.locationslot.LocationSlotRegistered` | `warehouse.facility.events` | `facility-layout` | `locationCode`, `zoneId`, `locationType`, `role` (default `Storage`), `activities` | increments the storage-position or station tally | `StorageCapacityConsumer`, `STORAGE_CAPACITY_CONSUMER_GROUP`, `warehouse.facility.events.dlq` |
| `com.warehouse.wms.facility-layout.locationslot.LocationSlotDecommissioned` | `warehouse.facility.events` | `facility-layout` | `locationCode` | decrements the same tally buckets | `StorageCapacityConsumer` (same) |
| `com.warehouse.wes.order-management.order.OrderAllocated` | `warehouse.order-management.events` | `order-management` | `order_id` (= `subject`), `promise_date`, `len(lines)`, CloudEvents `time` | upserts one `order_demand` row at site `DEMAND_SITE_ID`, last writer wins | `OrderDemandConsumer`, `DEMAND_CONSUMER_GROUP` (required, always on), `warehouse.order-management.events.dlq` |
| `com.warehouse.wes.order-management.order.OrderPartiallyAllocated` | `warehouse.order-management.events` | `order-management` | same as `OrderAllocated` | same | `OrderDemandConsumer` (same) |
| `com.warehouse.wms.inbound-receiving.dockappointment.DockAppointmentBooked` | `warehouse.inbound-receiving.events` | `inbound-receiving` | `appointment_id` (= `subject`), `window_start`, `window_end`, `len(asn_numbers)`, CloudEvents `time` | stores one `inbound_appointments` row at site `DEMAND_SITE_ID` through `inbound.Resolve` (last writer wins on `time`; a Cancelled wins a tie) | `InboundDemandConsumer`, `INBOUND_DEMAND_CONSUMER_GROUP` (required, always on), `warehouse.inbound-receiving.events.dlq` |
| `com.warehouse.wms.inbound-receiving.dockappointment.DockAppointmentCancelled` | `warehouse.inbound-receiving.events` | `inbound-receiving` | `appointment_id` (= `subject`), CloudEvents `time` | flips the row to `CANCELLED` (a tombstone with no window when it arrives before its Booked); the appointment stops being demand | `InboundDemandConsumer` (same) |
| `com.warehouse.wes.fulfillment-execution.task.TaskCompleted` | `warehouse.fulfillment.events` | `fulfillment-execution` | `task_id` (= `subject`), `task_type`, `duration_seconds` (optional), CloudEvents `time` | adds the completion to the `observed_capacity` bucket of (site `DEMAND_SITE_ID`, upper-cased `task_type`, minute of `time`) | `ObservedCapacityConsumer`, `OBSERVED_CAPACITY_CONSUMER_GROUP` (required, always on), `warehouse.fulfillment.events.dlq` |
| the four `com.warehouse.wes.warehouse-planning.capacityplan.*` types | `warehouse.warehouse-planning.analytics` | this context | the analytics payloads above | upserts `plan_facts` in the analytical database | `AnalyticsConsumer` in `cmd/planning-projector`, `ANALYTICS_CONSUMER_GROUP`, `warehouse.warehouse-planning.analytics.dlq` |

Ignored on purpose: `OrderRepromised` (cpt ids only, no new cutoff instant) and
every other type on the order-management topic; the other `inbound-receiving` types
(ASN, check-in, completion, receipt) and every other fulfillment-execution type; everything on
`process-path-management`'s topic (not consumed at all, ADR 0001 Addendum).

Source: `internal/domain/capacityplan/events.go`,
`internal/domain/processcapacity/events.go`,
`internal/adapters/outbound/kafka/encoder.go`, `analytics_encoder.go`,
`internal/adapters/inbound/kafka/labor_capacity_consumer.go`,
`storage_capacity_consumer.go`, `order_demand_consumer.go`,
`inbound_demand_consumer.go`, `observed_capacity_consumer.go`, `analytics_consumer.go`, `deadletter.go`, `cmd/api/main.go`, `cmd/api/runtime.go`,
`cmd/planning-projector/main.go`, `apis/asyncapi.yaml`.
