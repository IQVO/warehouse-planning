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
the diagram shows REST and names the MCP tool. The four REST POSTs sit behind
the `Idempotency-Key` middleware (ADR 0019; the diagrams below omit it: the key
row, the aggregate change and the outbox rows commit in one unit of work). The
capacity aggregates have **no** optimistic version column: concurrent publishes
serialize on a row lock instead. The requirement-side `PUT`s (ADR 0023, sections
10 and 11) take no Idempotency-Key and use a `version` column instead.

## 1. Register a process capacity constraint (REST / MCP)

```mermaid
sequenceDiagram
  autonumber
  actor Client
  participant H as HTTP handler or MCP tool
  participant UC as RegisterProcessCapacityConstraint
  participant PC as ProcessCapacity
  participant R as ProcessCapacityRepository
  participant OB as OutboxRepository
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
  UC->>OB: Insert(encoded events the aggregate raised, if any) in the same unit of work
  UC->>PC: EffectiveRate()
  UC-->>H: effective rate and binding constraint
  H-->>Client: 201 effective_rate, effective_unit, binding_constraint
```

Source: `internal/application/usecases/register_process_capacity_constraint.go`,
`internal/adapters/inbound/http/process_capacity_handler.go`,
`internal/adapters/inbound/mcp/tools.go`,
`internal/adapters/outbound/postgres/process_capacity_repository.go`.
Omits: JSON decoding errors (`400 malformed-json`) and the MCP-only
`non-positive-period` check of `period_seconds` before the use case. The
aggregate raises `ProcessCapacityRegistered` (first constraint) or
`ProcessCapacityChanged` (effective rate moved), and nothing otherwise. In
`cmd/api` and `cmd/mcp` the use case is wired with the outbox, the encoder and
the unit of work, so the save and the outbox rows commit or roll back as one
(`cmd/api/main.go`); a use case built without them saves and publishes
nothing.

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
  alt not CloudEvents, other type, bad payload, no site_code nor building_id, or no path_id
    L->>K: CommitMessages, skipped with a WARN
  else valid ShiftPlanCommitted
    L->>U: Do
    U->>P: Claim(labor-capacity-consumer, event id)
    alt already claimed
      P-->>U: false, nothing else happens
    else first time
      U->>UC: Handle(LABOR, upper(path_id), site_code or building_id, window from event time, heads x rate UNIT per hour)
      alt domain validation error
        UC-->>U: skipped with a WARN, claim kept
      end
      opt the line carries a shift_id
        U->>U: RecordShiftCommitment, the drift's commitment trigger (section 12)
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

## 4. Expected demand from OrderAllocated (Kafka)

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
  O->>O: order_id must equal subject, demand.NewOrder at site_id or else DEMAND_SITE_ID
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
`internal/domain/demand/order.go`, `cmd/api/runtime.go`.
Omits: the skip branches and the retry-then-DLQ path to
`warehouse.order-management.events.dlq` (as diagram 2). The consumer is always
on: `DEMAND_CONSUMER_GROUP` and `DEMAND_SITE_ID` are required at boot.

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
  Client->>H: POST /capacity-plans/{id}/publications or publish_capacity_plan
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
  else SUPERSEDED
    CP-->>UC: ErrSuperseded, transaction rolled back
    H-->>Client: 409 capacity-plan-superseded
  else DRAFT
    Note over CP: records Published, plus ShortageDetected and BottleneckDetected when shortage is above 0
    U->>R: FindPublished(site, location, path, window) FOR UPDATE
    Note over CP: an older PUBLISHED plan of the key: Supersede(by, at) records CapacityPlanSuperseded, saved BEFORE the newer plan
    U->>R: Save (a lost race on the unique index = ErrVersionConflict: rerun once, then 409 version-conflict)
    U->>E: Encode(PullEvents)
    U->>OB: Insert two rows per event
    U-->>UC: commit
    H-->>Client: 200 plan with status PUBLISHED
  end
```

Source: `internal/application/usecases/publish_capacity_plan.go`,
`internal/adapters/outbound/postgres/capacity_plan_repository.go`,
`internal/domain/capacityplan/capacity_plan.go`.
Omits: the in-memory test doubles (no row lock; the memory unit of work rolls
participants back on error), which no shipped binary uses.

## 8. Outbox relay to Kafka

```mermaid
sequenceDiagram
  autonumber
  participant Relay as outbox Relay in cmd/api
  participant DB as OutboxRepo Drain
  participant Sink as RelaySink
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
Omits: the lazy first dial of the Kafka writer (and the one-off
`Unknown Topic Or Partition` on the first write to an output topic that does not
exist yet). The log sink is gone ([ADR 0016](/docs/adr/0016-postgres-and-kafka-are-the-only-runtime)).

## 9. Analytics projection (cmd/planning-projector)

```mermaid
sequenceDiagram
  autonumber
  participant K as Kafka warehouse.warehouse-planning.analytics
  participant A as AnalyticsConsumer
  participant PR as analyticsstore Projection
  participant DLQ as analytics DLQ writer
  K->>A: FetchMessage
  A->>A: Decode, match one of the projected capacityplan types (CapacityPlanSuperseded is acknowledged, not projected)
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
WARN) and the read side (`cmd/planning-reports`), which only queries. The three
requirement-side types (ADR 0023) reach this consumer too and are acknowledged
without being projected.

## 10. Planner's morning, step 1: receive the workload forecast (REST)

```mermaid
sequenceDiagram
  autonumber
  actor Planner
  participant H as HTTP handler
  participant UC as ReceiveWorkloadForecast
  participant U as UnitOfWork
  participant R as WorkloadForecastRepository
  participant F as Forecast
  participant OB as OutboxRepository
  Planner->>H: PUT /workload-forecasts/{site_id}/{path_id} with buckets
  H->>H: decode, parse every cpt as RFC 3339
  alt not JSON, fractional quantity, bad cpt, bucket without cpt or quantity
    H-->>Planner: 400 malformed-json or malformed-cpt, 422 missing-cpt or missing-bucket-quantity
  end
  H->>UC: Handle(site_id, path_id, buckets)
  UC->>U: Do
  U->>R: Get(site_id, path_id)
  alt no stored forecast
    U->>F: New(site, path, buckets, now) version 1
  else stored
    U->>F: Receive(site, path, buckets, now)
    Note over F: equal buckets in the same order change nothing
  end
  alt validation error
    F-->>H: no-forecast-buckets, missing-cpt or negative-bucket-quantity
    H-->>Planner: 422
  else nothing changed
    U-->>H: Created false, Changed false, nothing written
    H-->>Planner: 200 stored forecast, version and received_at unchanged
  else created or changed
    U->>R: Save(forecast) guarded on the previous version
    U->>OB: Insert WorkloadForecastReceived on the events and analytics topics
    U-->>H: commit
    H-->>Planner: 201 version 1, or 200 version + 1
  end
  opt the version moved between Get and Save
    UC->>U: re-run the whole unit once
    UC-->>H: a second conflict is ErrVersionConflict
    H-->>Planner: 409 version-conflict
  end
```

Source: `internal/adapters/inbound/http/workload_forecast_handler.go`,
`internal/application/usecases/workload_forecast.go`,
`internal/domain/workloadforecast/forecast.go`,
`internal/adapters/outbound/postgres/workload_forecast_repository.go`.
Omits: the read (`GET /workload-forecasts/{site_id}/{path_id}`, a plain
`Get`, `404 workload-forecast-not-found`) and the relay hop to Kafka.

## 11. Planner's morning, step 2: commit the labour plan (REST, drift trigger B)

```mermaid
sequenceDiagram
  autonumber
  actor Planner
  participant H as HTTP handler
  participant UC as CommitLabourPlan
  participant U as UnitOfWork
  participant FR as WorkloadForecastRepository
  participant PR as LabourPlanRepository
  participant P as Plan
  participant T as StorageTallyReader
  participant CR as CommittedHeadsRepository
  participant OB as OutboxRepository
  Planner->>H: PUT /labour-plans/{site_id}/{shift_id}/{path_id}
  H->>UC: Handle(command, planned_heads maybe omitted)
  UC->>U: Do
  opt planned_heads omitted
    UC->>UC: check rate and hours first
    U->>FR: Get(site_id, path_id)
    UC->>UC: QuantityDueIn(shift_start, shift_start + hours)
    alt no shift_start, no forecast or zero buckets in the shift
      UC-->>H: ErrMissingPlannedHeads
      H-->>Planner: 422 missing-planned-heads
    else data in the shift
      UC->>UC: DeriveHeads = ceil(units / (rate x hours)), heads_source forecast
    end
  end
  U->>PR: Get(site_id, shift_id, path_id)
  alt no stored plan
    U->>P: New(spec, now) version 1
  else stored
    U->>P: Revise(spec, now)
  end
  alt invariant broken
    P-->>H: heads-exceed-stations, invalid-count, non-positive-rate, non-positive-hours or throughput-not-finite
    H-->>Planner: 422
  end
  U->>T: StationCount(site_id, upper(path_id))
  Note over UC,T: warning only when the tally knows at least one but fewer stations than installed_stations
  alt nothing changed
    H-->>Planner: 200 stored plan, version unchanged, no event, no drift check
  else created or changed
    U->>PR: Save(plan) guarded on the previous version
    U->>OB: Insert LabourPlanCommitted on the events and analytics topics
    U->>CR: Get(site_id, shift_id, path_id)
    opt a commitment is stored
      UC->>UC: CompareHeads(planned, committed), ShouldRaise(last drift)
      opt the drift is non-zero and differs from the last recorded one
        U->>OB: Insert LabourPlanDriftDetected
      end
      U->>CR: RecordDrift(drift, zero included)
    end
    U-->>H: commit
    H-->>Planner: 201 version 1, or 200 version + 1, with warnings
  end
```

Source: `internal/adapters/inbound/http/labour_plan_handler.go`,
`internal/application/usecases/labour_plan.go` (`spec`, `deriveHeads`,
`stationWarnings`), `labour_plan_drift.go` (`surfaceDrift`, `driftRecorder`),
`internal/domain/labourplan/plan.go`, `drift.go`, `cmd/api/requirement.go`.
Omits: the `409 version-conflict` path (as in section 10), the `400` decoding
errors (`malformed-json`, `malformed-shift-start`) and the reads
(`GET /labour-plans/...`, which recompute the warning on every call).

## 12. Labour plan drift, trigger A: a ShiftPlanCommitted line (Kafka)

The second step of the labor consumer's unit of work (section 2), after the
LABOR registration, whatever that registration decided.

```mermaid
sequenceDiagram
  autonumber
  participant L as LaborCapacityConsumer
  participant U as UnitOfWork
  participant UC as RecordShiftCommitment
  participant CR as CommittedHeadsRepository
  participant PR as LabourPlanRepository
  participant OB as OutboxRepository
  L->>UC: Handle(site, shift_id, CanonicalPathID(path_id), planned_heads as committed heads, event time)
  alt blank id, or heads below 0 or above 2147483647
    UC-->>L: rejection, logged and skipped, the LABOR step stays
  else valid
    UC->>U: Do, joining the consumer's open unit of work
    U->>CR: Get(site, shift, path), keeps the last recorded drift
    U->>CR: SaveCommitment(committed heads, observed_at)
    U->>PR: Get(site, shift, path)
    alt no labour plan for the key
      Note over U,PR: nothing compared, last drift stays unset until a plan is committed
    else plan stored
      UC->>UC: CompareHeads(planned, committed), ShouldRaise(last drift)
      opt the drift is non-zero and differs from the last recorded one
        U->>OB: Insert LabourPlanDriftDetected
      end
      U->>CR: RecordDrift(drift, zero included)
    end
  end
  Note over L,OB: the claim, the LABOR constraint, the commitment and the drift commit together
```

Source: `internal/adapters/inbound/kafka/labor_capacity_consumer.go`
(`recordCommitment`), `internal/application/usecases/labour_plan_drift.go`,
`internal/domain/labourplan/drift.go`, `commitment.go`,
`internal/adapters/outbound/postgres/committed_heads_repository.go`.
Omits: a line without `shift_id` (it records nothing here) and a redelivered
event (its claim fails first, so nothing runs). Accepted limits: there is no
stale-event guard on `observed_at` (per-key Kafka order is relied on), and if
the very first commitment and the very first plan of a key commit at the same
instant, neither trigger sees the other and one drift can go unraised until
the next change.
