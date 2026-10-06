---
id: class-diagram
title: Class diagrams
sidebar_label: Class diagrams
sidebar_position: 18
---

# Class diagrams

UML class diagrams of `internal/domain/**`, split by package, then the
hexagonal ports and the adapters that implement them. Field and method names
are the real Go identifiers (unexported fields keep their lower-case names);
only state-changing methods and the key queries are shown.

Stereotypes: `<<AggregateRoot>>`, `<<Entity>>`, `<<ValueObject>>`,
`<<Enumeration>>`, `<<DomainEvent>>`, `<<DomainService>>` (a plain Go function
drawn as a class) and `<<Repository>>` (an outbound port).

## Package `processcapacity`

```mermaid
classDiagram
  direction LR
  class ProcessCapacity {
    <<AggregateRoot>>
    -processType ProcessType
    -location string
    -window CapacityWindow
    -nativeUnit CapacityUnit
    -nativeUnitSet bool
    -order ConstraintType[]
    -constraints map of ConstraintType to CapacityRate
    +AddConstraint(constraintType, rate) error
    +EffectiveRate() CapacityRate, ConstraintType, error
    +Constraints() ConstraintEntry[]
  }
  class CapacityRate {
    <<ValueObject>>
    -quantity float64
    -unit CapacityUnit
    -period Duration
    +LessThan(other) bool
  }
  class CapacityWindow {
    <<ValueObject>>
    -start Time
    -end Time
    +Covers(other) bool
    +Duration() Duration
  }
  class ConstraintEntry {
    <<ValueObject>>
    +Type ConstraintType
    +Rate CapacityRate
  }
  class WorkloadProfile {
    <<ValueObject>>
    -unitsPerOrder float64
    -packagesPerOrder float64
    +NormalizeToOrderRate(rate) CapacityRate, error
  }
  class StationStandard {
    <<ValueObject>>
    -location string
    -processType ProcessType
    -perStation CapacityRate
    +CapacityFor(stationCount) CapacityRate, error
  }
  class CapacityUnit {
    <<Enumeration>>
    UNIT
    LINE
    ORDER
    PACKAGE
  }
  class ConstraintType {
    <<Enumeration>>
    LABOR
    LOCATION
    EQUIPMENT
    STATION
    CONVEYOR
    BUFFER
    REPLENISHMENT
  }
  class StepInput {
    <<ValueObject>>
    +Registered ProcessCapacity
    +Covering ProcessCapacity[]
    +Location string
    +StationCount int
    +Standard StationStandard
  }
  class StepResult {
    <<ValueObject>>
    +Step ProcessType
    +Rate CapacityRate
    +Binding ConstraintType
    +Warnings string[]
  }
  class PathCapacityResult {
    <<ValueObject>>
    +Rate CapacityRate
    +BottleneckStep ProcessType
    +BottleneckConstraint ConstraintType
    +Steps StepResult[]
    +Warnings string[]
  }
  class ComposeProcessPathCapacity {
    <<DomainService>>
    +ComposeProcessPathCapacity(path, inputs, profile) PathCapacityResult, error
    +ComposeStepCapacity(step, in, profile) StepResult, error
    +ComputeProcessPathCapacity(path, capacities, profile) CapacityRate, ProcessType, error
  }

  ProcessCapacity *-- CapacityWindow
  ProcessCapacity *-- "0..7" CapacityRate : per ConstraintType
  ProcessCapacity ..> ConstraintEntry : returns
  CapacityRate --> CapacityUnit
  ConstraintEntry --> ConstraintType
  StationStandard *-- CapacityRate
  StepInput o-- ProcessCapacity : covering aggregates
  StepInput o-- StationStandard
  PathCapacityResult *-- StepResult
  ComposeProcessPathCapacity ..> StepInput
  ComposeProcessPathCapacity ..> WorkloadProfile
  ComposeProcessPathCapacity ..> PathCapacityResult
```

Source: `internal/domain/processcapacity/process_capacity.go`,
`capacity_rate.go`, `capacity_window.go`, `workload_profile.go`,
`station_standard.go`, `step_capacity.go`, `process_path_capacity.go`.
Omits: getters, `SortNewestFirst`, the sentinel errors (listed on the
[aggregate design canvas](/docs/ddd/aggregate-design-canvas)) and
`ErrMissingStepCapacity`. `ProcessType` is a plain string type.

## Packages `capacityplan`, `processpath` and `demand`

```mermaid
classDiagram
  direction LR
  class CapacityPlan {
    <<AggregateRoot>>
    -id string
    -warehouseID string
    -location string
    -window CapacityWindow
    -processPathID string
    -assignedDemand float64
    -demandSource DemandSource
    -pathCapacity float64
    -bottleneckStep ProcessType
    -capacityOverWindow float64
    -shortage float64
    -bottleneckConstraint ConstraintType
    -warnings string[]
    -status Status
    -createdAt Time
    -publishedAt Time
    -events Event[]
    +Create(params, now)$ CapacityPlan, error
    +Rehydrate(params)$ CapacityPlan
    +Publish(now) error
    +PullEvents() Event[]
  }
  class Status {
    <<Enumeration>>
    DRAFT
    PUBLISHED
  }
  class DemandSource {
    <<Enumeration>>
    request
    orders
  }
  class Event {
    <<interface>>
    +EventName() string
    +AggregateID() string
    +OccurredAt() Time
  }
  class Header {
    <<ValueObject>>
    +PlanID string
    +At Time
  }
  class CapacityPlanCreated {
    <<DomainEvent>>
    +WarehouseID, Location, PathID
    +WindowStart, WindowEnd
    +AssignedDemand, PathCapacity
    +CapacityOverWindow, Shortage
    +BottleneckStep
  }
  class CapacityPlanPublished {
    <<DomainEvent>>
    +same fields as Created
    +BottleneckConstraint ConstraintType
  }
  class CapacityShortageDetected {
    <<DomainEvent>>
    +WarehouseID, Location, PathID
    +WindowStart, WindowEnd
    +AssignedDemand, CapacityOverWindow
    +Shortage, BottleneckStep
  }
  class BottleneckDetected {
    <<DomainEvent>>
    +WarehouseID, Location, PathID
    +WindowStart, WindowEnd
    +BottleneckStep, PathCapacity
  }
  class ProcessPath {
    <<Entity>>
    -id string
    -name string
    -steps ProcessType[]
    +NewProcessPath(id, name, steps)$ ProcessPath, error
    +Steps() ProcessType[]
  }
  class Order {
    <<Entity>>
    -id string
    -location string
    -promiseAt Time
    -releasedLines int
    -asOf Time
    +NewOrder(params)$ Order, error
    +Supersedes(prev) bool
    +CountsIn(start, end) bool
  }
  class Summary {
    <<ValueObject>>
    +Orders int
    +ReleasedLines int
    +AsOf Time
    +HasData() bool
  }

  CapacityPlan --> Status
  CapacityPlan --> DemandSource
  CapacityPlan *-- "0..*" Event : records until PullEvents
  CapacityPlan ..> ProcessPath : processPathID, by id only
  Event <|.. CapacityPlanCreated
  Event <|.. CapacityPlanPublished
  Event <|.. CapacityShortageDetected
  Event <|.. BottleneckDetected
  CapacityPlanCreated *-- Header
  CapacityPlanPublished *-- Header
  CapacityShortageDetected *-- Header
  BottleneckDetected *-- Header
  Summary ..> Order : Summarize
```

Source: `internal/domain/capacityplan/capacity_plan.go`, `events.go`,
`internal/domain/processpath/process_path.go`, `internal/domain/demand/order.go`.
Omits: `CreateParams` / `RehydrateParams` / `OrderParams`, getters, sentinel
errors, and `CapacityPlan`'s `CapacityWindow` (drawn in the first diagram).
Event fields are grouped per line for readability. `Order` is the entity of a
read model, not an aggregate root.

## Ports and adapters

```mermaid
classDiagram
  direction LR
  class ProcessCapacityRepository {
    <<Repository>>
    +Save(ctx, pc) error
    +FindByProcessLocationWindow(ctx, processType, location, start, end) ProcessCapacity
    +FindCovering(ctx, processType, location, start, end) ProcessCapacity[]
  }
  class CapacityPlanRepository {
    <<Repository>>
    +Save(ctx, plan) error
    +FindByID(ctx, id) CapacityPlan
  }
  class CapacityPlanLister {
    <<Repository>>
    +ListRecent(ctx, location, limit) CapacityPlan[]
  }
  class ProcessPathRepository {
    <<Repository>>
    +Save(ctx, path) error
    +FindByID(ctx, id) ProcessPath
  }
  class ProcessPathLister {
    <<Repository>>
    +List(ctx) ProcessPath[]
  }
  class StationStandardRepository {
    <<Repository>>
    +Save(ctx, standard) error
    +Find(ctx, location, processType) StationStandard
    +List(ctx, location) StationStandard[]
  }
  class StorageTallyRepository {
    <<Repository>>
    +RegisterSlot(ctx, locationCode, zoneID, tallyType, tallyKeys) Update[]
    +DecommissionSlot(ctx, locationCode) Update[], bool
  }
  class StorageTallyReader {
    <<Repository>>
    +StationCount(ctx, location, activity) int
    +SiteBuckets(ctx, location) Bucket[]
  }
  class OrderDemandRepository {
    <<Repository>>
    +Upsert(ctx, order) bool
    +Expected(ctx, location, start, end) Summary
  }
  class ProcessedEventRepository {
    <<Repository>>
    +Claim(ctx, consumer, eventID) bool
  }
  class OutboxRepository {
    <<Repository>>
    +Insert(ctx, msgs) error
  }
  class UnitOfWork {
    <<interface>>
    +Do(ctx, fn) error
  }
  class EventEncoder {
    <<interface>>
    +Encode(events) Message[]
  }
  class PlanMetrics {
    <<interface>>
    +CapacityPlanCreated(ctx, outcome)
  }
  class PostgresAdapters {
    postgres.ProcessCapacityRepo
    postgres.CapacityPlanRepo
    postgres.ProcessPathRepo
    postgres.StationStandardRepo
    postgres.StorageTallyRepo
    postgres.OrderDemandRepo
    postgres.ProcessedEventRepo
    postgres.OutboxRepo
    postgres.UnitOfWork
  }
  class MemoryAdapters {
    memory equivalents of every repo
    memory.UnitOfWork
  }
  class KafkaEncoders {
    kafka.Encoder
    kafka.AnalyticsEncoder
    kafka.FanoutEncoder
  }
  class TelemetryPlanMetrics {
    telemetry.PlanMetrics
  }

  ProcessCapacityRepository <|.. PostgresAdapters
  CapacityPlanRepository <|.. PostgresAdapters
  CapacityPlanLister <|.. PostgresAdapters
  ProcessPathRepository <|.. PostgresAdapters
  ProcessPathLister <|.. PostgresAdapters
  StationStandardRepository <|.. PostgresAdapters
  StorageTallyRepository <|.. PostgresAdapters
  StorageTallyReader <|.. PostgresAdapters
  OrderDemandRepository <|.. PostgresAdapters
  ProcessedEventRepository <|.. PostgresAdapters
  OutboxRepository <|.. PostgresAdapters
  UnitOfWork <|.. PostgresAdapters
  UnitOfWork <|.. MemoryAdapters
  OutboxRepository <|.. MemoryAdapters
  EventEncoder <|.. KafkaEncoders
  PlanMetrics <|.. TelemetryPlanMetrics
```

Source: `internal/application/ports/*.go`,
`internal/adapters/outbound/postgres/*.go`, `internal/adapters/outbound/memory/*.go`,
`internal/adapters/outbound/kafka/encoder.go`, `analytics_encoder.go`,
`internal/adapters/outbound/telemetry/metrics.go`.
Omits: the `ctx` / error result details, every memory implementation edge but
two (each port has a memory implementation), the analytics ports
(`report.Projection`, `report.Reader`, implemented by `analyticsstore`), which
sit outside the OLTP ports, and the relay's own `outbox.Store` / `outbox.Sink`
interfaces.

## Hexagonal view

```mermaid
flowchart LR
  subgraph Inbound adapters
    HTTP["inbound/http<br/>chi router, reports router"]
    MCPA["inbound/mcp<br/>11 tools"]
    KIN["inbound/kafka<br/>labor, storage, order-demand, analytics consumers"]
  end
  subgraph Application
    UC["usecases<br/>Register..., GetProcessPathCapacity,<br/>Create/PublishCapacityPlan, DeclareStationStandard,<br/>RecordOrderDemand, GetExpectedDemand, List..."]
    PORTS["ports<br/>repositories, UnitOfWork, EventEncoder, PlanMetrics"]
  end
  subgraph Domain
    DOM["processcapacity, capacityplan,<br/>processpath, demand"]
  end
  subgraph Analytics read side
    REP["analytics/report<br/>Projection, Reader ports"]
  end
  subgraph Outbound adapters
    PG["outbound/postgres + pgtx"]
    MEM["outbound/memory"]
    KOUT["outbound/kafka<br/>encoders, RelaySink"]
    RELAY["outbound/outbox<br/>relay"]
    TEL["outbound/telemetry"]
    AS["outbound/analyticsstore"]
  end
  HTTP --> UC
  MCPA --> UC
  KIN --> UC
  KIN --> PORTS
  UC --> PORTS
  UC --> DOM
  PORTS --> DOM
  PG -. implements .-> PORTS
  MEM -. implements .-> PORTS
  KOUT -. implements .-> PORTS
  TEL -. implements .-> PORTS
  RELAY --> KOUT
  KIN --> REP
  HTTP --> REP
  AS -. implements .-> REP
```

Source: `internal/architecture/architecture_test.go` (the dependency rule),
`cmd/api/main.go`, `cmd/mcp/main.go`, `cmd/planning-projector/main.go`,
`cmd/planning-reports/main.go`.
Omits: the composition roots in `cmd/*` (the only place layers are wired) and
the CloudEvents helper `internal/adapters/kafka/cloudevents` used by both Kafka
adapters. `KIN --> REP` is the analytics consumer writing through
`report.Projection`; `HTTP --> REP` is the reports router reading through
`report.Reader`; the OLTP layers never import either (arch-test). The
`KIN --> PORTS` edge is the storage consumer writing the tally and claims
without a use case.
