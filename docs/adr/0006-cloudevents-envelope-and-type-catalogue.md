# ADR 0006: CloudEvents 1.0 is the only envelope, and this is the type catalogue

## Status

Accepted (2026-10-05). Records a decision already in effect: every message this service publishes has been a
CloudEvents 1.0 structured-mode event since the outbox publisher was written. It had no ADR, which left the fleet's
type-catalogue fitness test (`TestEventCatalogueMatchesContract`) with nothing to compare `apis/asyncapi.yaml` against.

## Context

The fleet standard (`.claude/rules/fleet/cloudevents.md`) makes CloudEvents 1.0 mandatory on every Kafka message and fixes
the `type` naming. Other contexts learn what a service emits by reading its CloudEvents ADR and its AsyncAPI contract, so
the two must agree.

## Decision

1. CloudEvents 1.0 structured mode is the only envelope. There is no flat or dual mode and no envelope toggle.
2. `type` is `com.warehouse.wes.warehouse-planning.<entity>.<EventName>`, built by `cloudevents.Type(entity, ev.EventName())`
   in `internal/adapters/outbound/kafka/encoder.go`. The entity segment is the raising aggregate, lowercase:
   `capacityplan` or `processcapacity`.
3. `subject` is the aggregate id (`ev.AggregateID()`): the capacity plan id, or a ProcessCapacity's canonical identity
   `<processType>/<location>/<windowStart>/<windowEnd>`.
4. Each CapacityPlan event is published twice: as an integration event on `warehouse.warehouse-planning.events` and as
   an analytics record on `warehouse.warehouse-planning.analytics`. Both go through the transactional outbox.
   ProcessCapacity events (constraint registrations) are integration-stream only: the analytics stream is plan facts
   only (ADR 0005), and the analytics encoder refuses one rather than silently dropping it.

### This service's types

| topic | `type` | `subject` |
| --- | --- | --- |
| `warehouse.warehouse-planning.events` | `com.warehouse.wes.warehouse-planning.capacityplan.CapacityPlanCreated` | capacity plan id |
| `warehouse.warehouse-planning.events` | `com.warehouse.wes.warehouse-planning.capacityplan.CapacityPlanPublished` | capacity plan id |
| `warehouse.warehouse-planning.events` | `com.warehouse.wes.warehouse-planning.capacityplan.CapacityShortageDetected` | capacity plan id |
| `warehouse.warehouse-planning.events` | `com.warehouse.wes.warehouse-planning.capacityplan.BottleneckDetected` | capacity plan id |
| `warehouse.warehouse-planning.events` | `com.warehouse.wes.warehouse-planning.processcapacity.ProcessCapacityRegistered` | `<processType>/<location>/<windowStart>/<windowEnd>` |
| `warehouse.warehouse-planning.events` | `com.warehouse.wes.warehouse-planning.processcapacity.ProcessCapacityChanged` | `<processType>/<location>/<windowStart>/<windowEnd>` |
| `warehouse.warehouse-planning.analytics` | `com.warehouse.wes.warehouse-planning.capacityplan.CapacityPlanCreated` | capacity plan id |
| `warehouse.warehouse-planning.analytics` | `com.warehouse.wes.warehouse-planning.capacityplan.CapacityPlanPublished` | capacity plan id |
| `warehouse.warehouse-planning.analytics` | `com.warehouse.wes.warehouse-planning.capacityplan.CapacityShortageDetected` | capacity plan id |
| `warehouse.warehouse-planning.analytics` | `com.warehouse.wes.warehouse-planning.capacityplan.BottleneckDetected` | capacity plan id |

## Consequences

- A new event type is added in three places: the AsyncAPI contract, this table, and the publisher. The fleet fitness test
  `TestEventCatalogueMatchesContract` fails when the first two disagree.
- The test only sees types written out in full in the contract; it cannot check the topic or subject columns.
