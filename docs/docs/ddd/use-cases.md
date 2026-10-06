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
| `GetExpectedDemand` | query | The orders whose promise cutoff falls in `[start, end)` at a site, their released lines and the newest `as_of`, from the local read model. |
| `ListProcessPaths` | query | Every registered ProcessPath, ordered by id (REST only, for the console remote). |
| `ListCapacityPlans` | query | The most recently created plans, optionally for one location; default 20, capped at 100 (REST only). |

Plain reads without a use case (a repository port is called directly):
`GET /process-capacities` (the effective rate of one exact
`(process, location, window)` aggregate; there is no
`GetEffectiveProcessCapacity` type), `GET /capacity-plans/{id}` and
`GET /station-standards`.

The `LaborCapacityConsumer` calls `RegisterProcessCapacityConstraint` inside
its own unit of work (claim + upsert); the `StorageCapacityConsumer` calls no
use case and writes the tally through `ports.StorageTallyRepository`. How each
use case runs, step by step, is on the
[sequence diagrams](/docs/ddd/sequence-diagrams) page.

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
