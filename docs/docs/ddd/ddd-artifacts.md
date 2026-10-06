---
id: ddd-artifacts
title: DDD artifacts (ddd-crew)
sidebar_label: DDD artifacts index
sidebar_position: 10
---

# DDD artifacts (ddd-crew)

The strategic and tactical design of `warehouse-planning`, drawn with the
[ddd-crew](https://github.com/ddd-crew) tools plus UML and ER diagrams. Every
diagram is Mermaid, derived from the code on `develop`, and carries a
"Source:" line naming the files it came from.

| Artifact | ddd-crew tool / notation | What it shows |
| --- | --- | --- |
| [Core domain chart](/docs/ddd/core-domain-chart) | [Core Domain Charts](https://github.com/ddd-crew/core-domain-charts) | Where this context sits on business differentiation vs model complexity, and why it is Core. |
| [Bounded context canvas](/docs/ddd/bounded-context-canvas) | [Bounded Context Canvas v5](https://github.com/ddd-crew/bounded-context-canvas) | Purpose, classification, roles, every inbound and outbound message, business decisions. |
| [Context map](/docs/ddd/context-map) | [Context Mapping](https://github.com/ddd-crew/context-mapping) | This context's slice of the fleet map: upstream/downstream, patterns and technology per edge. |
| [Aggregate design canvas](/docs/ddd/aggregate-design-canvas) | [Aggregate Design Canvas v1.1](https://github.com/ddd-crew/aggregate-design-canvas) | `ProcessCapacity` and `CapacityPlan`: states, invariants, commands, events, size. |
| [Domain message flow](/docs/ddd/domain-message-flow) | [Domain Message Flow Modelling](https://github.com/ddd-crew/domain-message-flow-modelling) | Key business scenarios as numbered command / event / query flows across contexts. |
| [EventStorming](/docs/ddd/eventstorming) | [EventStorming glossary and cheat sheet](https://github.com/ddd-crew/eventstorming-glossary-cheat-sheet) | Design-level process flows with the sticky-note colours, plus hotspots. |
| [Ubiquitous language](/docs/ddd/ubiquitous-language) | Glossary | Every term with the code identifier that implements it. |
| [Class diagrams](/docs/ddd/class-diagram) | UML class diagram | Domain types per package, and the hexagonal ports with their adapters. |
| [Entity-relationship diagram](/docs/ddd/entity-relationship) | ER diagram | The final OLTP and analytical schemas, and which tables back which aggregate. |
| [Sequence diagrams](/docs/ddd/sequence-diagrams) | UML sequence diagram | Every command use case, from inbound adapter to outbox, with error branches. |
| [Domain events](/docs/ddd/domain-events) | Event catalogue | Every event published and consumed: full CloudEvents type, topic, key, payload, producer, consumers. |

Related pages outside the pack: [Use cases](/docs/ddd/use-cases),
[Aggregates](/docs/overview/aggregates),
[Capacity composition](/docs/overview/capacity-composition),
[Upstream contracts](/docs/ecosystem/upstream-contracts) and the
[event catalogue](/docs/api-reference/events).

:::note[Sources of truth]
The code wins. Domain rules come from `internal/domain/**`, the use cases from
`internal/application/usecases`, the wiring from `cmd/*/main.go`, the schema
from `internal/adapters/outbound/postgres/migrations/*.up.sql` and
`analytics/migrations/*.up.sql`, the REST contract from `apis/openapi.yaml`,
the event contract from `apis/asyncapi.yaml` and the encoders in
`internal/adapters/outbound/kafka`, and decisions from the ADRs in
`docs/adr/`. Where a page and the code disagree, the page is wrong.
:::
