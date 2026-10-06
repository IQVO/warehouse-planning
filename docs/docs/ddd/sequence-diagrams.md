---
id: sequence-diagrams
title: Sequence diagrams
sidebar_label: Sequence diagrams
sidebar_position: 20
---

# Sequence diagrams

UML sequence diagrams of every command use case, derived from the use-case
bodies in `internal/application/usecases`, the inbound adapters and the
Postgres adapters. REST and MCP call the **same** use cases; where both exist
the diagram shows REST and names the MCP tool. This service has **no**
`Idempotency-Key` middleware and **no** optimistic version column: concurrent
publishes serialize on a row lock instead.

## 1. Register a process capacity constraint (REST / MCP)

```mermaid
sequenceDiagram
  autonumber
  actor Client
  participant H as HTTP handler or MCP tool
  participant UC as RegisterProcessCapacityConstraint
  participant PC as ProcessCapacity
  participant R as ProcessCapacityRepository
  Client->>H: POST /process-capacities or register_process_capacity_constraint
  H->>UC: Handle(command)
  UC->>UC: NewCapacityWindow, NewCapacityRate
  alt invalid window or rate
    UC-->>H: ErrInvalidWindow, ErrNegativeQuantity or ErrNonPositivePeriod
    H-->>Client: 400 invalid-capacity-window or 422
  end
  UC->>R: FindByProcessLocationWindow(process, location, start, end)
  R-->>UC: aggregate or nil
  opt nil
    UC->>PC: NewProcessCapacity(process, location, window)
  end
  UC->>PC: AddConstraint(type, rate)
  alt unit differs from the native unit
    PC-->>UC: ErrUnitMismatch
    H-->>Client: 409 unit-mismatch
  end
  UC->>R: Save(aggregate) upserts parent and constraint rows
  UC->>PC: EffectiveRate()
  UC-->>H: effective rate and binding constraint
  H-->>Client: 201 effective_rate, effective_unit, binding_constraint
```

Source: `internal/application/usecases/register_process_capacity_constraint.go`,
`internal/adapters/inbound/http/process_capacity_handler.go`,
`internal/adapters/inbound/mcp/tools.go`,
`internal/adapters/outbound/postgres/process_capacity_repository.go`.
Omits: JSON decoding errors (`400 malformed-json`) and the MCP-only
`non-positive-period` check of `period_seconds` before the use case. There is
no unit of work on this REST path: `Save` runs its own transaction.

## 2. Labor capacity from ShiftPlanCommitted (Kafka)

```mermaid
sequenceDiagram
  autonumber
  participant K as Kafka warehouse.workforce.events
  participant L as LaborCapacityConsumer
  participant U as UnitOfWork
  participant P as ProcessedEventRepository
  participant UC as RegisterProcessCapacityConstraint
  participant D as DLQ writer
  K->>L: FetchMessage
  L->>L: cloudevents.Decode, match full type, DataAs
  alt not CloudEvents, other type, bad payload or missing building_id or path_id
    L->>K: CommitMessages, skipped with a WARN
  else valid ShiftPlanCommitted
    L->>U: Do
    U->>P: Claim(labor-capacity-consumer, event id)
    alt already claimed
      P-->>U: false, nothing else happens
    else first time
      U->>UC: Handle(LABOR, upper(path_id), building_id, window from event time, heads x rate UNIT per hour)
      alt domain validation error
        UC-->>U: skipped with a WARN, claim kept
      end
    end
    U-->>L: commit
    L->>K: CommitMessages
  end
  opt transient failure such as claim, find, save or commit
    L->>L: retry the same message with backoff 200ms doubling to 5s, up to 5 attempts
    L->>D: publish to warehouse.workforce.events.dlq with x-dlq headers
    L->>K: CommitMessages after the DLQ write succeeded
  end
```

Source: `internal/adapters/inbound/kafka/labor_capacity_consumer.go`,
`kafka.go` (`consumeLoop`), `deadletter.go`,
`internal/adapters/outbound/postgres/unit_of_work.go`, `processed_event_repository.go`.
Omits: the commit retry loop (an offset commit that fails is retried until it
succeeds) and logging.

## 3. Facility tally from LocationSlotRegistered / Decommissioned (Kafka)

```mermaid
sequenceDiagram
  autonumber
  participant K as Kafka warehouse.facility.events
  participant S as StorageCapacityConsumer
  participant U as UnitOfWork
  participant P as ProcessedEventRepository
  participant T as StorageTallyRepository
  K->>S: FetchMessage
  S->>S: Decode and validate, role defaults to Storage
  alt LocationSlotRegistered
    S->>U: Do
    U->>P: Claim(storage-capacity-consumer, event id)
    U->>T: RegisterSlot(locationCode, zoneId, LOCATION or STATION, keys)
    Note over T: no-op when the locationCode is already registered
  else LocationSlotDecommissioned
    S->>U: Do
    U->>P: Claim(storage-capacity-consumer, event id)
    U->>T: DecommissionSlot(locationCode)
    Note over T: untracked slot logged, never negative
  end
  U-->>S: commit, claim and tally together
  S->>K: CommitMessages
```

Source: `internal/adapters/inbound/kafka/storage_capacity_consumer.go`,
`internal/adapters/outbound/postgres/storage_tally_repository.go`.
Omits: the skip branches (not CloudEvents, unknown type, missing fields,
unknown role), the duplicate-claim branch and the retry-then-DLQ path to
`warehouse.facility.events.dlq`, all identical to diagram 2. No use case and no
`ProcessCapacity` are involved.

## 4. Expected demand from OrderAllocated (Kafka, opt-in)

```mermaid
sequenceDiagram
  autonumber
  participant K as Kafka warehouse.order-management.events
  participant O as OrderDemandConsumer
  participant UC as RecordOrderDemand
  participant U as UnitOfWork
  participant P as ProcessedEventRepository
  participant R as OrderDemandRepository
  K->>O: FetchMessage
  O->>O: Decode, match OrderAllocated or OrderPartiallyAllocated
  O->>O: order_id must equal subject, demand.NewOrder with DEMAND_SITE_ID
  O->>UC: Handle(order-demand-consumer, event id, order)
  UC->>U: Do
  U->>P: Claim
  alt duplicate
    P-->>UC: outcome duplicate
  else claimed
    U->>R: Upsert(order) locks the row FOR UPDATE
    R-->>UC: applied, or stale when an older event
  end
  U-->>O: commit
  O->>K: CommitMessages
```

Source: `internal/adapters/inbound/kafka/order_demand_consumer.go`,
`internal/application/usecases/expected_demand.go` (`RecordOrderDemand`),
`internal/adapters/outbound/postgres/order_demand_repository.go`,
`internal/domain/demand/order.go`, `cmd/api/demand.go`.
Omits: the skip branches and the retry-then-DLQ path to
`warehouse.order-management.events.dlq` (as diagram 2). The consumer only runs
when `DEMAND_CONSUMER_GROUP` is set.

## 5. Register a process path and declare a station standard (REST / MCP)

```mermaid
sequenceDiagram
  autonumber
  actor Client
  participant H as HTTP handler or MCP tool
  participant RP as RegisterProcessPath
  participant PR as ProcessPathRepository
  participant DS as DeclareStationStandard
  participant SR as StationStandardRepository
  Client->>H: POST /process-paths or register_process_path
  H->>RP: Handle(id, name, steps)
  RP->>RP: NewProcessPath
  alt empty steps
    H-->>Client: 422 empty-process-path-steps
  else valid
    RP->>PR: Save, replaces name and steps wholesale
    H-->>Client: 201 path
  end
  Client->>H: PUT /station-standards/SIM1/PACK or declare_station_standard
  H->>DS: Handle(location, process, quantity, unit, period)
  DS->>DS: NewCapacityRate, NewStationStandard
  alt zero quantity, LINE unit or blank key
    H-->>Client: 422 non-positive-station-standard, unsupported-normalization-unit or missing-station-standard-field
  else valid
    DS->>SR: Find(location, process)
    DS->>SR: Save, upsert on location and process type
    H-->>Client: 201 when created, 200 when replaced
  end
```

Source: `internal/application/usecases/register_process_path.go`,
`declare_station_standard.go`, `internal/adapters/inbound/http/process_path_handler.go`,
`station_handler.go`, `internal/domain/processpath/process_path.go`,
`internal/domain/processcapacity/station_standard.go`.
Omits: JSON decoding errors and the MCP result shapes.

## 6. Create a capacity plan (REST / MCP)

```mermaid
sequenceDiagram
  autonumber
  actor Client
  participant H as HTTP handler or MCP tool
  participant UC as CreateCapacityPlan
  participant ED as GetExpectedDemand
  participant PPC as GetProcessPathCapacity
  participant Repo as Repositories
  participant CP as CapacityPlan
  participant U as UnitOfWork
  participant E as FanoutEncoder
  participant OB as OutboxRepository
  Client->>H: POST /capacity-plans or create_capacity_plan
  H->>UC: Handle(command, DemandFromOrders when assigned_demand is absent)
  opt assigned_demand omitted
    UC->>ED: Handle(location, start, end)
    alt no orders in the window
      UC-->>H: ErrMissingAssignedDemand
      H-->>Client: 422 missing-assigned-demand
    end
  end
  UC->>UC: NewCapacityWindow
  UC->>PPC: Handle(path id, location, window, factors)
  PPC->>Repo: ProcessPaths.FindByID
  alt unknown path
    H-->>Client: 404 process-path-not-found
  end
  loop every step
    PPC->>Repo: FindCovering, Tally.StationCount, StationStandards.Find
  end
  PPC->>PPC: ComposeProcessPathCapacity, normalize to ORDER, minimum
  alt a step has no candidate
    H-->>Client: 422 missing-step-capacity
  end
  UC->>CP: Create(params, now) records CapacityPlanCreated
  UC->>U: Do
  U->>Repo: CapacityPlans.Save
  U->>E: Encode(PullEvents)
  E-->>U: integration row and analytics row, one CloudEvents id
  U->>OB: Insert both rows
  U-->>UC: commit, plan and outbox rows together
  UC->>UC: Metrics.CapacityPlanCreated(created or rejected)
  H-->>Client: 201 plan with status DRAFT
```

Source: `internal/application/usecases/create_capacity_plan.go`,
`get_process_path_capacity.go`, `expected_demand.go`,
`internal/domain/processcapacity/process_path_capacity.go`, `step_capacity.go`,
`internal/domain/capacityplan/capacity_plan.go`,
`internal/adapters/outbound/kafka/analytics_encoder.go`,
`internal/adapters/inbound/http/capacity_plan_handler.go`.
Omits: the WorkloadProfile errors (`422` factor slugs), `ErrNegativeDemand`,
`ErrRequiredField` and malformed-timestamp `400`s. The metric is recorded for
both outcomes.

## 7. Publish a capacity plan (REST / MCP)

```mermaid
sequenceDiagram
  autonumber
  actor Client
  participant H as HTTP handler or MCP tool
  participant UC as PublishCapacityPlan
  participant U as UnitOfWork
  participant R as CapacityPlanRepository
  participant CP as CapacityPlan
  participant E as FanoutEncoder
  participant OB as OutboxRepository
  Client->>H: POST /capacity-plans/{id}/publish or publish_capacity_plan
  H->>UC: Handle(id)
  UC->>U: Do
  U->>R: FindByID locks the row FOR UPDATE
  alt not found
    UC-->>H: ErrCapacityPlanNotFound
    H-->>Client: 404 capacity-plan-not-found
  end
  U->>CP: Publish(now)
  alt already PUBLISHED
    CP-->>UC: ErrAlreadyPublished, transaction rolled back
    H-->>Client: 409 capacity-plan-already-published
  else DRAFT
    Note over CP: records Published, plus ShortageDetected and BottleneckDetected when shortage is above 0
    U->>R: Save
    U->>E: Encode(PullEvents)
    U->>OB: Insert two rows per event
    U-->>UC: commit
    H-->>Client: 200 plan with status PUBLISHED
  end
```

Source: `internal/application/usecases/publish_capacity_plan.go`,
`internal/adapters/outbound/postgres/capacity_plan_repository.go`,
`internal/domain/capacityplan/capacity_plan.go`.
Omits: the in-memory repositories (no row lock; the memory unit of work rolls
participants back on error).

## 8. Outbox relay to Kafka

```mermaid
sequenceDiagram
  autonumber
  participant Relay as outbox Relay in cmd/api
  participant DB as OutboxRepo Drain
  participant Sink as RelaySink or LogSink
  participant K as Kafka
  loop every OUTBOX_RELAY_INTERVAL, immediately again after a full batch of 100
    Relay->>DB: Drain(limit 100)
    DB->>DB: SELECT unpublished ORDER BY id FOR UPDATE SKIP LOCKED
    loop each row in id order
      DB->>Sink: Send(message)
      Sink->>K: WriteMessages to message topic, key = plan id
      alt send failed
        DB->>DB: attempts + 1, last_error, commit what was sent, stop the pass
      else sent
        DB->>DB: published_at = now()
      end
    end
    DB->>DB: commit
  end
```

Source: `internal/adapters/outbound/outbox/relay.go`,
`internal/adapters/outbound/postgres/outbox_repository.go`,
`internal/adapters/outbound/kafka/relay_sink.go`, `cmd/api/main.go`
(`startOutboxRelay`).
Omits: `EVENT_PUBLISHER=log`, where `LogSink` logs and nothing reaches Kafka,
and the lazy first dial of the Kafka writer.

## 9. Analytics projection (cmd/planning-projector)

```mermaid
sequenceDiagram
  autonumber
  participant K as Kafka warehouse.warehouse-planning.analytics
  participant A as AnalyticsConsumer
  participant PR as analyticsstore Projection
  participant DLQ as analytics DLQ writer
  K->>A: FetchMessage
  A->>A: Decode, match one of the four capacityplan types
  alt known type with an unusable payload or a deterministic store rejection
    A->>DLQ: publish to warehouse.warehouse-planning.analytics.dlq
  else valid
    A->>PR: Apply(event) inserts analytics_processed_events and upserts plan_facts in one transaction
    Note over A,PR: a transient failure is retried on the same message forever, never dead-lettered
  end
  A->>K: CommitMessages
```

Source: `internal/adapters/inbound/kafka/analytics_consumer.go`,
`internal/adapters/outbound/analyticsstore/postgres_projection.go`,
`cmd/planning-projector/main.go`, ADR 0005.
Omits: not-CloudEvents and other-type messages (skipped with a rate-limited
WARN) and the read side (`cmd/planning-reports`), which only queries.
