# ADR 0004: Demand is ingested from order-management's published events into a local read model; `assigned_demand` becomes optional

## Status

Accepted (2026-10-04). Closes the "Phase 4 simplification" recorded on
`CreateCapacityPlanCommand` and in ADR 0001 (demand arrives in the request
body, final ingestion "a later decision"). Additive: nothing that worked
before changes.

## Context

`POST /capacity-plans` evaluates `assigned_demand` (orders) against a path's
capacity. Nothing supplied that number: the operator typed it.
`order-management` is the fleet's order front door and PUBLISHES CloudEvents
with order facts. This context's cross-context rule is unchanged: no live
REST/MCP call to a sibling, published events only, local read models.

### What the events actually carry (read from order-management's own
`apis/asyncapi.yaml` on `develop`, not guessed)

Its **integration** topic `warehouse.order-management.events` carries exactly
three types:

| type | meaning | payload (`data`) |
| --- | --- | --- |
| `...order.OrderAllocated` | every line allocated; eligible lines released in the same pass | `order_id`, **`promise_date`** (the promise cutoff instant; for a multi-group order the LATEST group's cutoff), `lines[]` (exactly the lines released in this pass, may be empty; each `line_no`, `sku`, `path_id`, `gift_wrap`, `fulfillment_class`, optional per-line `promise_cutoff_at`), optional `promise_basis` / `promise_cpt_id` |
| `...order.OrderPartiallyAllocated` | some lines allocated and released, some backordered (partial-shipment orders only) | same shape |
| `...order.OrderRepromised` | the promise of a shipment group moved | `order_id`, `reason`, optional `cpt_id_old` / `cpt_id_new` -- **no new instant** |

CloudEvents `subject` = the order id, `time` = occurred-at, key = order id.
Its **analytics** topic (`warehouse.order-management.analytics`) additionally
carries `OrderReceived` (`line_count`), `OrderCancelled`, `OrderReleased`, ...
but that topic is the analytics data product's stream (its own ADR 0006: the
projector is its sole consumer), not an integration contract.

What is therefore **not** available on the integration contract:

- **No fulfillment site.** Neither `Order`/`OrderLine` nor any event carries
  one. order-management's ADR 0031 says so explicitly and matches its
  planned-capacity read model against ONE configured site
  (`PLANNED_CAPACITY_SITE_ID`, defaulting to its `DEFAULT_SITE_ID`).
- **No quantities.** A line has no unit count; there is no `units` figure to
  sum. `lines[]` counts lines, not units.
- **No cancellations or de-allocations.** `OrderCancelled` is analytics-only.
  (Its BR6 -- order-management ADR 0004 -- also makes cancellation legal only
  while no line has reached `Released`, and `OrderAllocated` releases every
  eligible line in the same pass, so a cancelled order that was already
  announced on the integration topic is the narrow case of an allocation whose
  lines were all ineligible.)
- **No new cutoff on a re-promise.** `OrderRepromised` names CPT ids, not an
  instant.

## Decision

### 1. A local read model fed ONLY by Kafka, in orders

`internal/domain/demand` holds the rules; `order_demand` (migration `0006`,
purely additive) holds **one row per order id**: `order_id`, `location`,
`promise_at`, `released_lines`, `as_of`. There is no call to order-management
anywhere; a plan it never published an order for has no demand here.

### 2. What "expected demand in window W at site L" means

> The number of **distinct orders** attributed to site `L` whose **promise
> cutoff** (`promise_date`) lies in the half-open window **`[W.start, W.end)`**
> -- a cutoff exactly at `W.start` counts, exactly at `W.end` does not (so
> adjacent windows never count an order twice) -- counted **once per order id**,
> at the cutoff of its **latest** event.

- **Last writer wins per order id on the CloudEvents `time`**
  (`demand.Order.Supersedes`): a later **or equal** `time` replaces the stored
  row (so a re-allocation that moves the promise moves the order between
  windows, and a same-time redelivery under a new id converges instead of
  depending on arrival order); an older one is a no-op.
- `released_lines` is returned beside `orders` as **`released_lines`** and is
  explicitly **not units**. **Units are not supported**: no event carries them,
  and inventing one (e.g. `lines x something`) would be a made-up number.
- The figure `POST /capacity-plans` uses is **`orders`** -- the plan's unit is
  already ORDER (`assigned_demand` is in orders, `path_capacity` in
  ORDER/hour).
- **No data is not zero.** A window with no order (or a site with none) yields
  `orders = 0`; for the plan that is *no data*, never a silent zero (decision 6).
- `as_of` = the newest `time` among the events the site's model reflects (whole
  site, not only the window), so a reader can judge staleness.

### 3. Events consumed and events ignored

Consumed (dispatch on the **full** `type`, topic `warehouse.order-management.events`):

- `com.warehouse.wes.order-management.order.OrderAllocated`
- `com.warehouse.wes.order-management.order.OrderPartiallyAllocated`

Both carry `promise_date` and the released lines; both upsert the order. An
order with zero released lines is still an order (the promise is what places
it).

Ignored, and why:

- `OrderRepromised`: carries no new cutoff instant, so it cannot move an order
  between windows. Consequence (accepted, documented): after a re-promise the
  model keeps the order at its original cutoff until a later
  `OrderAllocated`/`OrderPartiallyAllocated` for the same order (e.g. a retried
  allocation) replaces it.
- **Cancellations / de-allocations: not supported, so the model is NOT netted.**
  They are not on the integration topic. Consuming `OrderCancelled` would mean
  subscribing to the analytics data product's stream, a second topic and
  consumer for an event that BR6 makes unreachable for a released order. The
  model therefore **can overstate** demand by the orders announced and then
  cancelled before release. A test that "a cancelled order lowers demand" is
  deliberately absent: the capability does not exist.
- `OrderReceived`/`OrderReleased`/`OrderLine*`/`OrderCancelled` (analytics
  topic only), per-line `promise_cutoff_at` (the order-level `promise_date` is
  used: one cutoff per order, so one order is one count), and any unknown type.

Validation (anything failing is **skipped with a WARN, offset committed**,
never retried, never blocking the partition): not a CloudEvents 1.0 event
(including the retired flat envelope), malformed payload, blank `order_id`,
`subject != data.order_id`, missing/unparsable `promise_date`, missing event
`time`. Validation runs **before** the transaction opens.

### 4. The site assumption (stated, not hidden)

order-management orders carry **no fulfillment site**. We do **not** invent a
per-order site (no SKU prefix, no reservation-id parsing). Exactly like
order-management's own ADR 0031, **all consumed demand is attributed to ONE
configured site**: env **`DEMAND_SITE_ID`** (e.g. `SIM1`, the `location` this
service already uses for the labor and facility data). It is stored on each row
as `location` at write time. If the events later carry a usable site, the
consumer should read it and `DEMAND_SITE_ID` becomes a fallback. If
`DEMAND_SITE_ID` differs from the `location` a plan uses, the plan finds no
demand and the 422 below applies -- it never defaults silently. Changing
`DEMAND_SITE_ID` leaves previously written rows under the old site (they stop
being read for the new one; replaying the topic under a new group re-attributes
the history).

### 5. The consumer

`internal/adapters/inbound/kafka/order_demand_consumer.go`, the labor and
storage consumers' shape exactly:

- **Atomic**: `usecases.RecordOrderDemand` runs the processed-event claim
  (`processed_events`, consumer `order-demand-consumer`, keyed on the
  CloudEvents `id`) and the upsert in **ONE `ports.UnitOfWork.Do`**; a failure
  after the claim rolls the claim back so the redelivery is processed, not
  skipped.
- `FetchMessage` / `CommitMessages`: the offset is committed **only after**
  success; a transient (infrastructure) error returns non-nil and the same
  message is retried with capped exponential backoff (200ms to 5s); deterministic
  problems return nil.
- **Consumer group from env `DEMAND_CONSUMER_GROUP`, no default, and that env
  var is the feature's off switch**: unset means no consumer, no Kafka reader,
  no extra query -- start-up and behaviour are byte-for-byte what they were. A
  group without `DEMAND_SITE_ID` refuses to boot. Stable shared group; a group
  without a committed offset starts from the earliest retained message, so a
  fresh deployment rebuilds the model from history.
- Started in `cmd/api` only; `cmd/mcp` never dials Kafka and reads the same
  Postgres table.

### 6. Reads and the plan default (all additive)

- `GET /demand?location=&window_start=&window_end=` and MCP tool
  `get_expected_demand` return `orders`, `released_lines`, `source`
  (`order-management`) and `as_of` (null when nothing is known), RFC 7807
  errors, no auth. The tool budget is raised 10 to 11 deliberately.
- `POST /capacity-plans` / `create_capacity_plan`: **`assigned_demand` is
  optional.** Present (including `0`) it is used exactly as stated -- it
  **always wins** -- and the response says `demand_source: "request"`. Omitted:
  the plan uses the number of orders the read model expects at `(location,
  window)` and says `demand_source: "orders"`; `assigned_demand` in the response
  is that figure. Omitted and the model has **no order** for that location and
  window (or is not wired): **today's 422 `missing-assigned-demand`**,
  byte-identical, with the same precedence as before. Never a silent zero.
  `demand_source` is persisted (`capacity_plans.demand_source`, default
  `request`, which is true for every earlier plan) and is **not** part of any
  published event: event payloads are untouched.

## Why a local read model (and what was rejected)

- **Live REST/MCP call to order-management at plan time** -- rejected: a hard
  fleet rule, and a capacity decision must stay available if the upstream is
  degraded.
- **Per-process in-memory cache with full replay** -- rejected: the demand must
  be queryable from every replica and from `cmd/mcp` (a separate deployable)
  without a warm-up, and the claim must be atomic with the write.
- **Consume the analytics topic** (gets cancellations, `line_count`) -- rejected:
  it is the analytics data product's stream, not an integration contract; it
  would add a second topic/consumer to approximate a quantity order-management
  does not publish, for an event BR6 makes nearly unreachable.
- **Per-order site inferred from SKU / reservation / order id** -- rejected as
  invention; see decision 4.
- **Units = lines x a factor** -- rejected as invention; the WorkloadProfile
  factors already convert ORDERS to units on the capacity side.
- **Netting by treating `OrderRepromised` as a removal** -- rejected: the order is
  still demand, only later.
- **`assigned_demand` as `*float64` in the use case command** -- rejected: it
  would change the public command type and every existing test; an additive
  `DemandFromOrders` flag set by the adapters when the field is absent keeps the
  explicit path byte-identical.

## The feedback loop (read before extending)

order-management consumes THIS service's capacity events (its ADR 0031: a
published `CapacityShortageDetected` annotates the orders whose promise window
overlaps the shortage, at ITS configured site) and this service consumes
order-management's order events (this ADR). The two data flows are
**independent and neither writes into the other's aggregates**: order-management
only annotates and never moves a promise because of a plan, and this service
only reads orders into its own read model and never changes an order.
Consequently a shortage cannot reduce the demand that produced it (the
annotation does not change `promise_date`, so the order stays counted); there is
no cycle to oscillate. The loop is a *feedback of information*, closed by a
human who re-plans. If order-management ever moves promises because of a
planned shortage (its ADR 0031 lists that as a separate future ADR), THIS
model would shift demand between windows in response and a new ADR must reason
about convergence before that ships.

Both services use the same single-site simplification, so with
`DEMAND_SITE_ID` equal to order-management's `PLANNED_CAPACITY_SITE_ID` (the
fleet uses `SIM1`) the two views describe the same site.

## Consequences

- Planners stop typing demand when order data exists, and see where a number
  came from (`demand_source`).
- **Overstatement**: cancellations are not netted; re-promised orders keep their
  last announced cutoff; `released_lines` of a partially allocated order covers
  the latest pass only. Use `GET /demand`'s `as_of` and `orders` as an input, not
  as a guarantee.
- Postgres: migration `0006_order_demand` (new table + `capacity_plans.demand_source`,
  down migration drops both). It runs on boot of `cmd/api` and `cmd/mcp` (advisory
  lock; idempotent).
- Deployment: `DEMAND_CONSUMER_GROUP` and `DEMAND_SITE_ID` (chart values
  `demand.consumerGroup` / `demand.siteId`) turn the consumer on; a non-empty
  `demand.consumerGroup` requires `demand.siteId` and `kafka.enabled`.

## Verification

- Domain (`gremlins`, 13/13 killed): the half-open predicate at start, end and one
  second either side; adjacent windows never double-count; `Supersedes`
  older/equal/newer; validation at the boundary values.
- Handler tests: valid event updates the model; replay of the same id with a
  different payload is a no-op; unknown types (incl. a right suffix under the
  wrong owner) ignored; legacy-flat/garbage skipped without error and without
  opening a unit of work; invalid payloads skipped before the transaction; a
  transient failure after the claim returns an error, leaves nothing written and
  the claim un-recorded, and the retry succeeds; last-writer-wins on event time;
  the run loop commits only after success.
- Use-case tests: request wins (explicit 0 included); orders used; no data /
  other site / cutoff exactly at the window end / no model wired -> 422; read
  failures are not masked.
- godog scenarios through the real HTTP surface with events fed through the
  consumer's `HandleMessage` (`features/expected_demand.feature`).
- `-tags=integration`: testcontainers **Kafka and Postgres** -- a real event lands
  in the model, the replay and junk write nothing, and a plan created without
  `assigned_demand` picks it up; a failure after the claim is retried, not lost;
  SQL boundaries equal the in-memory adapter's; migration `0006` down/up.
