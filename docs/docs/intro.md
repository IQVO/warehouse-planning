---
id: intro
title: Introduction
slug: /intro
sidebar_position: 1
---

# Warehouse Planning

`warehouse-planning` is a **Core** bounded context in the `warehouse-systems`
fleet (GitHub org `IQVO`). It answers one question:

> *Can this warehouse process the demand assigned to it, given its current
> labor, location, equipment, station, conveyor and buffer constraints?*

No other fleet context computes normalized, cross-process effective capacity
or forward-looking capacity shortages. Inventory Management answers "what do
we have"; this context answers "how much work can we perform".

## Where it sits

| Property | Value |
| --- | --- |
| Subdomain classification | Core Domain ([ADR 0001](/docs/adr/0001-warehouse-planning-bounded-context); reasoning and neighbours on [Subdomain classification](/docs/ddd/subdomain-classification)) |
| Tier | `wes` (CloudEvents type prefix `com.warehouse.wes.warehouse-planning.*`; `wms` is the tier of `facility-layout`, `inventory-storage`, `product-master`, `inbound-receiving` and `slotting-optimization`) |
| Language / style | Go backend, hexagonal architecture (ports and adapters) |
| Inbound adapters | REST (`cmd/api`, `:8080`) and MCP (`cmd/mcp`, `:8090`, Streamable HTTP) |
| Integration | Kafka CloudEvents 1.0 (structured mode), transactional outbox for publishing |
| Auth | None on REST or MCP (fleet-wide revert of 2026-09-11) |

:::note Study project
Like the rest of the `warehouse-systems` fleet, this is a personal study
project exploring Domain-Driven Design, hexagonal architecture and AI-agent
harness engineering. It is not production software and carries no support
guarantee.
:::

## What this context owns

- **ProcessCapacity** - the usable throughput of one warehouse process at one
  location for one time window: the minimum across its registered constraints.
- **CapacityPlan** - assigned demand for a location and planning window,
  compared with the capacity of a process path, producing a shortage (if any)
  and a bottleneck.
- **ProcessPath** - an ordered sequence of process types (for example Pick,
  Rebin, Pack). It is *locally declared* by an operator; it is not copied from
  `process-path-management`.
- **StationStandard** - the operator-declared throughput of one station of a
  process at a site. No upstream publishes it.

It also keeps an **expected-demand read model** (one row per
`order-management` order, fed only by Kafka; [ADR 0004](/docs/adr/0004-demand-ingestion-from-order-management))
that a capacity plan defaults its demand to when the caller omits it.

It does **not** own labor scheduling (`workforce-management`), storage
slotting and layout (`facility-layout`), stock levels (`inventory-storage`),
SKU master data such as handling classification and unit dimensions
(`product-master`) or path capability and eligibility authoring
(`process-path-management`).

## Where to go next

Understand the model:

- [Bounded context](/docs/overview/context): purpose, context map, the no-live-lookup rule.
- [Aggregates](/docs/overview/aggregates): `ProcessCapacity` and `CapacityPlan` and their invariants.
- [Capacity composition](/docs/overview/capacity-composition): how a step's capacity is resolved (window coverage, station capacity).
- [Runtime](/docs/overview/runtime): the four binaries and how they are wired.
- [Use cases](/docs/ddd/use-cases) and [Subdomain classification](/docs/ddd/subdomain-classification).
- [DDD artifacts](/docs/ddd/ddd-artifacts): the ddd-crew pack (core domain chart, canvases, context map, EventStorming, class, ER and sequence diagrams).
- [Integration contracts](/docs/ecosystem/upstream-contracts): every upstream, downstream and caller, with failure behaviour.

Run and operate it:

- [Quickstart](/docs/overview/quickstart): build, test and run locally, first calls.
- [Configuration](/docs/operations/configuration): every environment variable per binary.
- [Runbook](/docs/operations/runbook), [Observability](/docs/operations/observability) and [Troubleshooting](/docs/operations/troubleshooting).
- [Testing](/docs/development/testing): the test pyramid, `make` targets and CI jobs.

Reference:

- [API reference](/docs/api-reference): REST (generated from `apis/openapi.yaml`) and the event catalogue.
- [MCP tools](/docs/mcp/tools): every tool of `cmd/mcp`.
- [Architecture decision records](/docs/adr): the index of all twelve ADRs.
