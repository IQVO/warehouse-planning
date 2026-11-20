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
the `ProcessCapacity` aggregate (`internal/domain/processcapacity/events.go`),
the `WorkloadForecast` aggregate (`internal/domain/workloadforecast/events.go`)
and the `LabourPlan` aggregate and its drift (`internal/domain/labourplan/events.go`),
encoded by `internal/adapters/outbound/kafka/encoder.go` and inserted into
`outbox_events` inside the use case's unit of work; the relay in `cmd/api`
publishes them. Common attributes: `source=/warehouse/warehouse-planning`,
`subject` = Kafka key = **partition key** = the raising aggregate's id
(`kafkago.Hash` balancer) — the capacity plan id,
`<processType>/<location>/<windowStart>/<windowEnd>` for a ProcessCapacity,
`<site_id>/<path_id>` for a workload forecast and
`<site_id>/<shift_id>/<path_id>` for a labour plan and its drift,
`time` = domain occurred-at,
`dataschema=urn:warehouse:warehouse-planning:events:<EventName>:v1`.

| Full CloudEvents type | Topic | Producer use case | Payload (`data`) | Known consumers |
| --- | --- | --- | --- | --- |
| `com.warehouse.wes.warehouse-planning.capacityplan.CapacityPlanCreated` | `warehouse.warehouse-planning.events` | `CreateCapacityPlan` | `plan_id`, `warehouse_id`, `location`, `path_id`, `window_start`, `window_end`, `assigned_demand`, `path_capacity`, `capacity_over_window`, `shortage`, `bottleneck_step`, `status` (always `DRAFT`) | `order-management` (`planned_capacity_consumer.go`, opt-in) |
| `com.warehouse.wes.warehouse-planning.capacityplan.CapacityPlanPublished` | `warehouse.warehouse-planning.events` | `PublishCapacityPlan`, always | the Created fields without `status`, plus `published_at` and (additively, ADR 0012) `site_id` — a facility-layout Site `site_code`, omitted for plans stored before migration 0008 | `order-management` |
| `com.warehouse.wes.warehouse-planning.capacityplan.CapacityShortageDetected` | `warehouse.warehouse-planning.events` | `PublishCapacityPlan`, only when `shortage > 0` | `plan_id`, `warehouse_id`, `location`, `path_id`, `window_start`, `window_end`, `assigned_demand`, `capacity_over_window`, `shortage`, `bottleneck_step` | `order-management` |
| `com.warehouse.wes.warehouse-planning.capacityplan.BottleneckDetected` | `warehouse.warehouse-planning.events` | `PublishCapacityPlan`, only when `shortage > 0` | `plan_id`, `warehouse_id`, `location`, `path_id`, `window_start`, `window_end`, `bottleneck_step`, `path_capacity` | none (`order-management` ignores it) |
| `com.warehouse.wes.warehouse-planning.capacityplan.CapacityPlanSuperseded` | `warehouse.warehouse-planning.events` and `.analytics` | `PublishCapacityPlan`, when another PUBLISHED plan exists for the same `(site_id, location, path_id, window)` (ADR 0024 decision 3); `subject` and the Kafka key are the OLDER plan's id | `plan_id` (the older plan), `superseded_by`, `warehouse_id`, `site_id`, `location`, `path_id`, `window_start`, `window_end` | `order-management` (terminal `SUPERSEDED` row, its ADR 0052); the other consumers ignore it |
| `com.warehouse.wes.warehouse-planning.processcapacity.ProcessCapacityRegistered` | `warehouse.warehouse-planning.events` | `RegisterProcessCapacityConstraint`, when the FIRST constraint of a (process, location, window) is registered | `process_type`, `location`, `window_start`, `window_end`, `constraint_type`, `quantity`, `unit`, `period_seconds` | none yet |
| `com.warehouse.wes.warehouse-planning.processcapacity.ProcessCapacityChanged` | `warehouse.warehouse-planning.events` | `RegisterProcessCapacityConstraint`, when a registration moves the effective rate (value, binding, or both) | `process_type`, `location`, `window_start`, `window_end`, `constraint_type`, `previous_quantity`, `previous_unit`, `previous_period_seconds`, `previous_binding_constraint`, `effective_quantity`, `effective_unit`, `effective_period_seconds`, `effective_binding_constraint` | none yet |
| `com.warehouse.wes.warehouse-planning.workloadforecast.WorkloadForecastReceived` | `warehouse.warehouse-planning.events` and `.analytics` | `ReceiveWorkloadForecast` (`PUT /workload-forecasts/{site_id}/{path_id}`), when the receipt created or changed the buckets (ADR 0023 decision 1) | `site_id`, `path_id`, `total_quantity`, `bucket_count`, `received_at`, `version` (totals, not the buckets) | none yet (`x-no-consumer-reason` in the AsyncAPI) |
| `com.warehouse.wes.warehouse-planning.labourplan.LabourPlanCommitted` | `warehouse.warehouse-planning.events` and `.analytics` | `CommitLabourPlan` (`PUT /labour-plans/{site_id}/{shift_id}/{path_id}`), when the commit created or changed the plan (ADR 0023 decision 2) | `site_id`, `shift_id`, `path_id`, `planned_heads`, `rate_units_per_hour`, `hours`, `installed_stations`, `shift_start` (omitted, never null, when the plan has none), `heads_source`, `version` | `workforce-management` (its ADR 0043: a `labour_requirements` copy, applied only when `version` is greater than the stored one; group `workforce-management-labour-plans`) |
| `com.warehouse.wes.warehouse-planning.labourplan.LabourPlanDriftDetected` | `warehouse.warehouse-planning.events` and `.analytics` | `RecordShiftCommitment` (a `ShiftPlanCommitted` line) or `CommitLabourPlan` (a changed plan), when the drift is non-zero and differs from the last one recorded for the key (ADR 0023 decision 3) | `site_id`, `shift_id`, `path_id`, `planned_heads`, `committed_heads`, `drift_heads` (= committed - planned, never 0) | none yet (`x-no-consumer-reason` in the AsyncAPI) |

Units: `assigned_demand`, `capacity_over_window` and `shortage` are orders;
`path_capacity` is ORDER per hour; `total_quantity` is units;
`rate_units_per_hour` is units per head per hour; times are RFC 3339 UTC.

A `PUT` that changes nothing (the same buckets in the same order, or the same
plan) writes nothing and raises nothing, so the requirement events are a
change log, not a heartbeat. The drift event never changes the labour plan or
the commitment it compares.

Event order on a plan with a shortage: Created (at creation), then at publish, in this order in the outbox:
Superseded (of the older plan of the same key, when there is one), Published, ShortageDetected,
BottleneckDetected.
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

The three requirement-side types (ADR 0023) are written to both topics with
the same payload; the projector acknowledges them without projecting them
(no report reads them yet).

## Consumed

Consumers decode with `cloudevents.Decode`, dispatch on the full `type`, ignore
every other type, and dedupe on the CloudEvents `id` in `processed_events`
inside the same unit of work as the effect.

| Full CloudEvents type | Topic | Producer | Fields used | Effect | Consumer, group env, DLQ |
| --- | --- | --- | --- | --- | --- |
| `com.warehouse.wes.workforce-management.shiftplan.ShiftPlanCommitted` | `warehouse.workforce.events` | `workforce-management` | `site_code` (else the deprecated `building_id`, ADR 0021), `path_id`, `planned_heads`, `planned_rate`, `planned_hours`, `shift_id`, CloudEvents `time` | (1) upserts a LABOR constraint `planned_heads x planned_rate` UNIT/HOUR on `ProcessCapacity(upper(path_id), site_code, [time, time + planned_hours))`; (2) in the same unit of work, when the line carries a `shift_id`, records `planned_heads` as the committed heads of `(site, shift_id, path_id)` (path id trimmed and upper-cased by `labourplan.CanonicalPathID`) and surfaces the labour plan drift (ADR 0023 decision 3) | `LaborCapacityConsumer`, `LABOR_CAPACITY_CONSUMER_GROUP`, `warehouse.workforce.events.dlq` |
| `com.warehouse.wms.facility-layout.locationslot.LocationSlotRegistered` | `warehouse.facility.events` | `facility-layout` | `locationCode`, `zoneId`, `locationType`, `role` (default `Storage`), `activities` | increments the storage-position or station tally | `StorageCapacityConsumer`, `STORAGE_CAPACITY_CONSUMER_GROUP`, `warehouse.facility.events.dlq` |
| `com.warehouse.wms.facility-layout.locationslot.LocationSlotDecommissioned` | `warehouse.facility.events` | `facility-layout` | `locationCode` | decrements the same tally buckets | `StorageCapacityConsumer` (same) |
| `com.warehouse.wes.order-management.order.OrderAllocated` | `warehouse.order-management.events` | `order-management` | `order_id` (= `subject`), `promise_date`, `len(lines)`, optional `site_id`, CloudEvents `time` | upserts one `order_demand` row at the event's `site_id`, else at `DEMAND_SITE_ID` (ADR 0021), last writer wins | `OrderDemandConsumer`, `DEMAND_CONSUMER_GROUP` (required, always on), `warehouse.order-management.events.dlq` |
| `com.warehouse.wes.order-management.order.OrderPartiallyAllocated` | `warehouse.order-management.events` | `order-management` | same as `OrderAllocated` | same | `OrderDemandConsumer` (same) |
| `com.warehouse.wms.dock-scheduling.dockappointment.DockAppointmentBooked` | `warehouse.dock-scheduling.events` | `dock-scheduling` | `appointment_id` (= `subject`), `site_id`, `direction`, `window_start`, `window_end`, `len(asn_numbers)`, `version` | stores one `inbound_appointments` row at the event's `site_id` through `inbound.Resolve` (state only when `version` is greater than the stored one; direction and ASN count filled in when missing); only INBOUND rows are demand | `DockAppointmentConsumer`, `DOCK_APPOINTMENT_CONSUMER_GROUP` (required, always on), `warehouse.dock-scheduling.events.dlq` |
| `com.warehouse.wms.dock-scheduling.dockappointment.DockAppointmentRescheduled` | `warehouse.dock-scheduling.events` | `dock-scheduling` | `appointment_id` (= `subject`), `site_id`, `window_start`, `window_end`, `version` | moves the row's window (version-guarded); a Rescheduled that arrives before its Booked is a partial row that is not demand until the Booked supplies the direction | `DockAppointmentConsumer` (same) |
| `com.warehouse.wms.dock-scheduling.dockappointment.DockAppointmentCancelled` | `warehouse.dock-scheduling.events` | `dock-scheduling` | `appointment_id` (= `subject`), `site_id`, `version` | flips the row to `CANCELLED` (a tombstone with no window when it arrives before its Booked); the appointment stops being demand | `DockAppointmentConsumer` (same) |
| `com.warehouse.wms.dock-scheduling.dockappointment.DockAppointmentNoShow` | `warehouse.dock-scheduling.events` | `dock-scheduling` | `appointment_id` (= `subject`), `site_id`, `direction`, `version` | flips the row to `NO_SHOW`; the appointment stops being demand | `DockAppointmentConsumer` (same) |
| `com.warehouse.wes.fulfillment-execution.task.TaskCompleted` | `warehouse.fulfillment.events` | `fulfillment-execution` | `task_id` (= `subject`), `task_type`, `duration_seconds` (optional), CloudEvents `time` | adds the completion to the `observed_capacity` bucket of (site `DEMAND_SITE_ID`, upper-cased `task_type`, minute of `time`) | `ObservedCapacityConsumer`, `OBSERVED_CAPACITY_CONSUMER_GROUP` (required, always on), `warehouse.fulfillment.events.dlq` |
| `com.warehouse.wes.labor-performance.standard.LaborStandardDefined` | `warehouse.labor-performance.events` (its integration topic, labor-performance ADR 0038) | `labor-performance` | `standard_id`, `task_type`, `expected_seconds` (> 0), `effective_from`, optional `travel_component_seconds`, `handling_unit`, `site_code`; CloudEvents `time` as `as_of` | inserts one `labor_standards` row per `standard_id` (a known id changes nothing); the standard in force is resolved at read time (ADR 0022) | `LaborStandardConsumer` (processed-event consumer `labor-standard-consumer`), `LABOR_STANDARD_CONSUMER_GROUP` (required; chart default `warehouse-planning-labor-standards`), `warehouse.labor-performance.events.dlq` |
| `com.warehouse.wes.labor-performance.standard.LaborStandardRevised` | `warehouse.labor-performance.events` | `labor-performance` | as Defined; `previous_expected_seconds` and the team-lift fields are ignored | a new row under the new `standard_id`, like a definition | `LaborStandardConsumer` (same) |
| the four projected `com.warehouse.wes.warehouse-planning.capacityplan.*` types (`CapacityPlanSuperseded` is acknowledged, not projected) | `warehouse.warehouse-planning.analytics` | this context | the analytics payloads above | upserts `plan_facts` in the analytical database | `AnalyticsConsumer` in `cmd/planning-projector`, `ANALYTICS_CONSUMER_GROUP`, `warehouse.warehouse-planning.analytics.dlq` |

Ignored on purpose: `OrderRepromised` (cpt ids only, no new cutoff instant) and
every other type on the order-management topic; the other `dock-scheduling` types (door calendar events), the retired
`inbound-receiving` appointment types (no longer consumed, [ADR 0020](/docs/adr/0020-inbound-demand-from-dock-scheduling)) and every other fulfillment-execution type; everything on
`process-path-management`'s topic (not consumed at all, ADR 0001 Addendum).

Source: `internal/domain/capacityplan/events.go`,
`internal/domain/processcapacity/events.go`,
`internal/domain/workloadforecast/events.go`, `internal/domain/labourplan/events.go`,
`internal/adapters/outbound/kafka/encoder.go`, `analytics_encoder.go`,
`internal/adapters/inbound/kafka/labor_capacity_consumer.go`,
`labor_standard_consumer.go`,
`storage_capacity_consumer.go`, `order_demand_consumer.go`,
`inbound_demand_consumer.go`, `observed_capacity_consumer.go`, `analytics_consumer.go`, `deadletter.go`, `cmd/api/main.go`, `cmd/api/runtime.go`,
`cmd/planning-projector/main.go`, `apis/asyncapi.yaml`.
