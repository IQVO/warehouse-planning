---
id: runtime
title: Runtime and integration mechanics
sidebar_position: 4
---

# Runtime and integration mechanics

## Layers

Hexagonal architecture, enforced by architecture tests in
`internal/architecture`:

| Layer | Path |
| --- | --- |
| Domain | `internal/domain/{processcapacity,capacityplan,processpath}` |
| Application (use cases, ports) | `internal/application/...` |
| Inbound adapters | `internal/adapters/inbound/{http,kafka,mcp}` |
| Outbound adapters | `internal/adapters/outbound/{postgres,memory,kafka,outbox}` |
| Composition roots | `cmd/api`, `cmd/mcp` |

## Publishing: transactional outbox

There is no dual write. `CreateCapacityPlan` and `PublishCapacityPlan` each run
one unit of work that saves the aggregate **and** inserts the already encoded
CloudEvents into `outbox_events`. The CloudEvents `id` is minted once at
encoding and persisted, so a relay retry republishes the same bytes and id.

The relay in `cmd/api` drains the table every `OUTBOX_RELAY_INTERVAL` (default
`1s`), claiming rows `FOR UPDATE SKIP LOCKED` in id order, sending one at a
time and stopping at the first failure, so a later event for a plan never
overtakes an earlier one. Delivery is at-least-once; consumers dedupe on `id`.
`EVENT_PUBLISHER=kafka|log` (default `log`) selects the Kafka sink or a log
sink; `kafka` requires `KAFKA_BROKERS`. Kafka is dialled lazily by the first
send, never at boot.

## Consuming: at-least-once with an atomic effect

- Offsets are committed only after a message was handled successfully
  (`FetchMessage` plus `CommitMessages`); transient failures retry the same
  message with capped exponential backoff.
- The processed-event claim (keyed by consumer and CloudEvents id) and every
  side effect commit or roll back together in one unit of work, so a
  rolled-back handling un-claims and the redelivery is processed.
- Only transient or infrastructure failures return an error. Deterministic
  problems (not a CloudEvent, unknown type, malformed payload, duplicate id,
  domain-validation rejections) are skipped.
- Consumer group ids come from environment variables
  (`LABOR_CAPACITY_CONSUMER_GROUP`, `STORAGE_CAPACITY_CONSUMER_GROUP`), never
  from string literals.

Known trade-off recorded in the repository rules: a message that fails with an
unrecognised but actually deterministic error blocks its partition (retried with
an ERROR log per attempt) rather than being silently dropped. There is no DLQ
yet.

## CloudEvents envelope

Every Kafka message is a CloudEvents 1.0 event in structured content mode with
the header `content-type: application/cloudevents+json; charset=UTF-8`. See the
[event catalogue](/docs/api-reference/events) for attributes and types.

## MCP

`cmd/mcp` exposes 10 tools (budget is 10): `register_process_capacity_constraint`,
`get_effective_process_capacity`, `register_process_path`,
`get_process_path_capacity`, `create_capacity_plan`, `publish_capacity_plan`,
`get_capacity_plan`, `declare_station_standard`, `list_station_standards` and
`get_storage_capacity`. Arguments are snake_case, matching the REST bodies;
failures are MCP tool errors whose text is `<slug>: <message>` using the REST
problem slugs.
