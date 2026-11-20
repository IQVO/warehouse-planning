---
id: use-cases
title: Use cases
sidebar_position: 2
---

# Use cases

Application services in `internal/application/usecases`. REST and MCP call the
same use cases.

| Use case | Kind | What it does |
| --- | --- | --- |
| `RegisterProcessCapacityConstraint` | command | Upserts one constraint on a ProcessCapacity aggregate, recomputes and persists the effective rate. |
| `RegisterProcessPath` | command | Declares (or wholesale replaces) a ProcessPath: id, name and an ordered, non-empty step list. |
| `GetProcessPathCapacity` | query | Resolves each step's covering ProcessCapacity aggregates, the site's tallied station counts and declared StationStandards, plus the WorkloadProfile; returns the normalized path capacity, bottleneck step and binding constraint, per-step breakdown and warnings. |
| `DeclareStationStandard` | command | Validates and upserts a StationStandard; reports created versus replaced. |
| `GetStorageCapacity` | query | The facility tally of a site as a read model (storage positions per zone and location type, stations per zone and activity). |
| `CreateCapacityPlan` | command | Resolves the demand (the stated `assigned_demand`, or the orders `GetExpectedDemand` reports when it is omitted), resolves the path capacity through the same read-time composition, builds the aggregate, saves it and queues `CapacityPlanCreated` in the outbox (two rows: integration and analytics topic), in one unit of work. Records the Tier-2 counter `warehouse_planning.capacity_plans.created` (ADR 0011). |
| `PublishCapacityPlan` | command | Loads the plan, publishes it, saves it and queues every recorded event in the outbox, in one unit of work. The Postgres lookup locks the row (`FOR UPDATE`) inside the unit of work, so concurrent publishes serialize. |
| `RecordOrderDemand` | command (Kafka only) | Claims the CloudEvents id and upserts one `demand.Order` in one unit of work; reports `applied`, `duplicate` or `stale` (an older event for a known order). Called by `OrderDemandConsumer`. |
| `RecordLaborStandard` | command (Kafka only) | Claims the CloudEvents id and stores one labour-standard fact (one row per `standard_id`; a repeated id is a no-op) in one unit of work. Called by `LaborStandardConsumer` (ADR 0022). |
| `GetExpectedDemand` | query | The orders whose promise cutoff falls in `[start, end)` at a site, their released lines and the newest `as_of`, from the local read model. |
| `ListProcessPaths` | query | Every registered ProcessPath, ordered by id (REST only, for the console remote). |
| `ListCapacityPlans` | query | The most recently created plans, optionally for one location; one keyset page `{items,nextCursor}`: `limit` default 100, max 500 (REST only). |
| `ReceiveWorkloadForecast` | command | ADR 0023 decision 1. Loads the forecast of `(site_id, path_id)`, builds it (version 1) or replaces its buckets (version + 1); a receipt equal to the stored buckets writes nothing. When something changed it saves the forecast and queues `WorkloadForecastReceived` (integration and analytics rows) in one unit of work. A version conflict re-runs the unit once; a second is `409 version-conflict`. REST only. |
| `GetWorkloadForecast` | query | The stored forecast of `(site_id, path_id)`, buckets in receipt order, or `404 workload-forecast-not-found`. REST only. |
| `CommitLabourPlan` | command | ADR 0023 decision 2. Uses the sent `planned_heads` (`heads_source` `request`) or derives them from the stored forecast: `ceil(units with cpt in [shift_start, shift_start + hours) / (rate x hours))` (`heads_source` `forecast`), refusing with `422 missing-planned-heads` when there is no `shift_start`, no forecast or no bucket in the shift. Builds or revises the plan; an equal plan writes nothing. When it changed: saves it, queues `LabourPlanCommitted`, and compares the heads with the stored commitment (drift trigger B), all in one unit of work. Every answer carries the station warning computed against the facility tally. REST only. |
| `GetLabourPlan`, `ListLabourPlans` | query | One plan of `(site_id, shift_id, path_id)` (`404 labour-plan-not-found`), or the plans of one `(site_id, shift_id)` ordered by path id, one keyset page; each with its station warning computed on read. REST only. |
| `RecordShiftCommitment` | command (Kafka only) | ADR 0023 decision 3, drift trigger A. Upserts the heads workforce-management committed for `(site, shift, path)` and, when a labour plan exists for the key, compares them, queues `LabourPlanDriftDetected` when the drift is non-zero and differs from the last recorded one, and records the drift (zero included). Runs on the `LaborCapacityConsumer`'s open unit of work after the LABOR registration; never changes the plan. |

Plain reads without a use case (a repository port is called directly):
`GET /process-capacities` (the effective rate of one exact
`(process, location, window)` aggregate; there is no
`GetEffectiveProcessCapacity` type), `GET /capacity-plans/{id}` and
`GET /station-standards`.

The `LaborCapacityConsumer` calls `RegisterProcessCapacityConstraint` and then,
for a line with a `shift_id`, `RecordShiftCommitment` inside its own unit of
work (claim + upsert + committed heads); the `StorageCapacityConsumer` calls no
use case and writes the tally through `ports.StorageTallyRepository`. How each
use case runs, step by step, is on the
[sequence diagrams](/docs/ddd/sequence-diagrams) page.

The requirement-side use cases (ADR 0023) are wired in `cmd/api` only
(`cmd/api/requirement.go`): no MCP tool and no A2A skill calls them (ADR 0023
decision 7).

:::note[Demand ingestion (docs/adr/0004)]
`assigned_demand` is optional on `POST /capacity-plans` (and the
`create_capacity_plan` MCP tool): an explicit value in the request body always
wins, and when omitted it is resolved from the expected-demand read model fed
by order-management's published `OrderAllocated`/`OrderPartiallyAllocated`
events (`GetExpectedDemand`). There is still no live cross-context lookup
-- the read model is populated by `OrderDemandConsumer`, never by a
synchronous call. WorkloadProfile factors (units/packages per order)
still arrive in the request body; no WorkloadProfile persistence exists
yet.
:::
