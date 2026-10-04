---
name: how-to-add-an-integration-event
description: Publish or consume a cross-service Kafka event in warehouse-planning - CloudEvents 1.0 type naming, internal/adapters/kafka/cloudevents helper, outbound Encoder + transactional outbox, inbound consumers with processed-event claims, apis/asyncapi.yaml, consumer-group rules. Use when touching internal/adapters kafka or outbox code, a consumer, or apis/asyncapi.yaml.
---

# How to add an integration event (publish and consume)

Kafka is ONE broker platform-wide. The rules that matter most are short and
live in `.claude/rules/fleet/cloudevents.md` and
`.claude/rules/fleet/kafka-testing-and-consumers.md` (do not edit those);
this repo's specifics, field by field, are in
`.claude/rules/integration-events.md`. This guide is the recipe.

Real code to read alongside it:

- Publish side: `internal/adapters/outbound/kafka/encoder.go` (`Encoder`),
  `internal/application/usecases/publish_capacity_plan.go` (the use case that
  enqueues), `internal/adapters/outbound/outbox/relay.go` (the relay).
- Consume side: `internal/adapters/inbound/kafka/labor_capacity_consumer.go`
  (`LaborCapacityConsumer`, workforce-management's `ShiftPlanCommitted`) and
  `storage_capacity_consumer.go` (facility-layout slot events).

## Publishing a new integration event

### 1. Is it actually cross-service?

Only `CapacityPlan` events go on the wire today: `CapacityPlanCreated`,
`CapacityPlanPublished`, `CapacityShortageDetected`, `BottleneckDetected`, on
`warehouse.warehouse-planning.events` (see `docs/adr/0001-warehouse-planning-bounded-context.md`
for the context map). `ProcessCapacityRegistered` / `ProcessCapacityChanged`
are vocabulary in `.claude/rules/domain-model.md` only: nothing raises them.
Before adding an event, confirm a sibling context needs to react to it. A
new subscriber relationship or a changed payload is a cross-repo contract
change: update the AsyncAPI document, the ADR and every consumer in one
coordinated set of PRs.

### 2. Domain event first

In `internal/domain/capacityplan/events.go`: an `Event<Name>` constant, a
struct embedding `Header` (plan id + occurred-at), and an `EventName()`
method. The aggregate (`capacity_plan.go`) records it in `Create`/`Publish`,
and `PullEvents()` hands it over once. The domain stays free of Kafka and
JSON. Add the aggregate test (`capacity_plan_test.go`), including the
"not raised when the condition is false" case (`CapacityShortageDetected`
is only recorded when `Shortage > 0`).

### 3. Encode through the ONE helper, store in the outbox

The envelope is built ONLY by `internal/adapters/kafka/cloudevents`
(`cloudevents.New(cloudevents.Spec{...})`, `Type`, `DataSchema`,
`ContentTypeHeader`, `Decode`); no hand-built envelope, no flat shape. In
`internal/adapters/outbound/kafka/encoder.go`:

1. add a snake_case payload struct (`planCreatedData` is the model; business
   types only, times in UTC);
2. add a `case` for the new event in `payloadFor` (an event it does not know
   is a hard error, so nothing silently goes missing);
3. keep `Entity = "capacityplan"` (the AGGREGATE that raised it, lowercase, no
   separators), `schemaVersion = 1`, and the topic constant
   `warehouse.warehouse-planning.events`. Resulting `type` =
   `com.warehouse.wes.warehouse-planning.capacityplan.<EventName>` (this
   context is `wes` tier); `dataschema` =
   `urn:warehouse:warehouse-planning:events:<EventName>:v1`; Kafka key and
   `subject` = the plan id.

The use case does the rest, inside ONE `ports.UnitOfWork.Do` (the outbox row
commits with the aggregate, so there is no dual write): save the plan, then
`enqueue(ctx, uc.Encoder, uc.Outbox, plan.PullEvents())`. The CloudEvents
`id` is minted once in `Encoder.Encode` and persisted, so a relay retry
republishes the same id. Never write to Kafka from a handler or use case;
the relay (`internal/adapters/outbound/outbox`, started in `cmd/api`, sink
`outbound/kafka/relay_sink.go`) does it, with `EVENT_PUBLISHER=kafka|log`
(default `log`, which needs no broker).

A breaking payload change is a new `.v2` type and dataschema, never a
mutation of an existing one.

### 4. Contract and docs

- `apis/asyncapi.yaml`: the message under this service's channel with the
  exact `type` const and `dataschema` (CI `api-lint` runs
  `spectral lint apis/asyncapi.yaml --ruleset .spectral.asyncapi.yaml --fail-severity=warn`).
- There is NO generated AsyncAPI HTML in this repo: the generator under
  `docs/` only produces the REST reference. Update the hand-written summary
  `docs/docs/api-reference/events.md`, the "Published types" table in
  `.claude/rules/integration-events.md`, and the domain events list in
  `.claude/rules/domain-model.md` in the same change.
- Build the site before opening the PR (broken links throw):
  `cd docs && npm ci && npm run build`.

### 5. Test

- A golden exact-JSON test per published `type` in
  `internal/adapters/outbound/kafka/encoder_test.go` (`TestGolden_PublishedTypes`
  asserts every attribute, the `type` and the `content-type` header; the
  `Encoder.NewID` hook pins the id).
- Use-case and handler tests assert the outbox content through the memory
  `OutboxRepo` (`assertOutboxTypes` in `capacity_plan_handler_test.go`,
  `the outbox event types are ...` in `features/capacity_plan.feature`).
- Delivery tests that need a real broker are `-tags=integration` and use
  testcontainers only (`internal/adapters/outbound/outbox/relay_integration_test.go`,
  `postgres/capacity_plan_outbox_integration_test.go`).

## Consuming an integration event from a sibling context

### 1. Never import the sibling's Go packages

The consumer knows the topic name, the exact CloudEvents `type` string and
a hand-mirrored payload struct, nothing else. Look at the constants and
`shiftPlanCommittedData` in `labor_capacity_consumer.go`: the `type` string
is copied byte for byte from the producer's own `apis/asyncapi.yaml` (read it
on the producer's `origin/develop`; never guess it, see the ADR 0001
Addendum). Adding a consumed type is a cross-repo contract decision: record
it in `docs/adr/` (ADR 0001 Addendum is the precedent) and in
`.claude/rules/integration-events.md`'s "Consumed types" table.
`process-path-management` is deliberately NOT consumed.

### 2. Handle one message exactly the way the existing consumers do

`HandleMessage(ctx, value)` in `labor_capacity_consumer.go` is the template:

```go
e, err := cloudevents.Decode(value) // validates CloudEvents 1.0
if err != nil { /* WARN log */ return nil } // never crash, never block the partition
if e.Type() != typeShiftPlanCommitted { return nil } // FULL type string, unknown types ignored
// e.DataAs(&data); validate required fields before opening the transaction
return c.UoW.Do(ctx, func(ctx context.Context) error {
	claimed, err := c.ProcessedEvents.Claim(ctx, laborConsumerName, e.ID())
	// !claimed -> already processed, return nil; then apply the effect
})
```

- Return nil for every deterministic problem (not a CloudEvent, unknown
  type, malformed payload, domain-validation rejection via
  `usecases.IsDomainValidationError`); return an error ONLY for transient
  infrastructure failures, so the loop retries the same message.
- The processed-event `Claim` and the side effect commit in ONE
  `UnitOfWork.Do`. Never claim in a separate statement before the work.
- The loop (`consumeLoop` in `kafka.go`) uses `FetchMessage` + `CommitMessages`
  and commits only after success. Never switch to `ReadMessage`, and leave
  `CommitInterval` unset on these durable-group readers (`readerConfig`).
- Add a use case or reuse an existing one for the effect
  (`usecases.RegisterProcessCapacityConstraint`); do not duplicate aggregate
  logic in the consumer. A new consumer name for `Claim` namespaces its
  idempotency rows.
- Wire it in `startKafkaConsumers` in `cmd/api/main.go`: no `KAFKA_BROKERS`
  means ingestion is disabled, never fatal; the reader dials lazily.

### 3. Consumer group ids

This repo's consumers are durable single-purpose groups (labor, storage)
whose group id comes from an env var with a named default:
`LABOR_CAPACITY_CONSUMER_GROUP`, `STORAGE_CAPACITY_CONSUMER_GROUP`
(`startKafkaConsumers`). Add the same for a new consumer; never an inline
`GroupID: "literal"` (`TestKafkaConsumerGroupNeverHardcodedInline` fails CI).
Because the defaults are shared names, a process run locally against the
cluster broker joins the SAME group as the in-cluster Deployment and one of
the two silently starves: set the env var to a unique value for any local
run against a shared broker.

If you ever add a consumer that rebuilds a local cache by replaying a topic's
full history on every start, that is a different pattern: it needs a
per-process-unique group id (a `uniqueConsumerGroup()`-style function) and an
explicit `CommitInterval` (`TestReplayConsumersSetCommitInterval`); see
`.claude/rules/fleet/kafka-testing-and-consumers.md`. No such consumer exists
here today.

### 4. Tests

- Unit: `labor_capacity_consumer_test.go` drives `HandleMessage` with a
  `fakeReader` (no broker): happy path, unknown type ignored, malformed
  message skipped, a legacy flat envelope REJECTED
  (`TestLaborCapacityConsumer_LegacyFlatEnvelope_Skipped`), replay of the
  same id not double counted.
- Atomicity and error classification: `atomic_labor_consumer_test.go`,
  `atomic_storage_consumer_test.go`; loop ordering/backoff:
  `consume_loop_test.go`, `run_loop_test.go`.
- Real delivery: `*_integration_test.go` in the same package, with the shared
  testcontainers Kafka broker from `main_integration_test.go`
  (`startKafkaBroker`), a unique topic per test, `createTopic` +
  `publishMessages`, and Postgres via testcontainers
  (`startPostgresForKafkaTests`). Never skip on `KAFKA_BROKERS`, never
  hardcode `localhost:9092`.

## Verify before opening the PR

```bash
make check-all   # includes arch-test (CloudEvents-only, group-id, testcontainers fitness tests)
make integration # needs Docker; real Kafka + Postgres via testcontainers
```
