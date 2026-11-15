---
description: "ADR 0021: order demand is attributed to the optional site_id of order-management's allocation events (its ADR 0050), with DEMAND_SITE_ID as the fallback; the labour consumer reads site_code before the deprecated building_id."
---

# ADR 0021: Order demand and labour capacity read the event's own site

## Status

Accepted (2026-11-15). Amends [ADR 0004](./0004-demand-ingestion-from-order-management.md)
§4 (the site assumption) and the site attribution of the labour consumer
recorded in [ADR 0001](./0001-warehouse-planning-bounded-context.md)'s Addendum.
Additive: no topic, no CloudEvents type, no request or response shape, no
migration and no environment variable changes. `DEMAND_SITE_ID` stays required.

## Context

ADR 0004 §4 attributed every consumed order to ONE configured site
(`DEMAND_SITE_ID`) because order-management's allocation events carried no
fulfillment site, and it stated the exit: "If the events later carry a usable
site, the consumer should read it and `DEMAND_SITE_ID` becomes a fallback."

That day has come. order-management's ADR 0050 (2026-11-06) added an optional
top-level `site_id` (string, `omitempty`) to `OrderAllocated` and
`OrderPartiallyAllocated` on `warehouse.order-management.events`: the order's
caller-assigned fulfillment site (its ADR 0044). An order without a site still
publishes exactly the pre-0050 bytes, the `dataschema` stays `:v1`, and
`OrderRepromised` carries no site. The pinned copy of that spec in
`internal/adapters/inbound/kafka/testdata/` already carries the
`orderAllocatedWithSite` and `orderPartiallyAllocatedWithSite` examples, but
the consumer kept attributing them to `DEMAND_SITE_ID`, so the demand of every
sited order was counted at the wrong site.

Separately, workforce-management's `ShiftPlanCommitted` now names its site in
`site_code` (canonical, optional because it was added after v1 shipped) and
keeps `building_id` as a DEPRECATED twin with the same value. The labour
consumer read only `building_id`.

Two attributions are NOT affected and stay on `DEMAND_SITE_ID`:
fulfillment-execution's `TaskCompleted` still carries no site (ADR 0017
decision 3), and orders without a site still need one.

## Decision

1. **Order demand reads `site_id`.** `OrderDemandConsumer` decodes the optional
   `site_id` of `OrderAllocated` / `OrderPartiallyAllocated`. When it is present
   and not blank, the order's `location` is that value with surrounding space
   trimmed; it is otherwise used exactly as given (no case folding, no pattern
   check: order-management owns the value). When it is absent or blank, the
   order is attributed to `DEMAND_SITE_ID`, as before.
2. **The site moves with the order.** The read model keeps one row per order id
   and last writer wins on the CloudEvents `time` (ADR 0004 §3). A newer
   allocation of the same order at another site therefore moves the order to
   that site; an older one changes nothing.
3. **Labour reads `site_code` first.** `LaborCapacityConsumer` uses
   `site_code` as the constraint's `location` and falls back to `building_id`
   only when `site_code` is absent or blank. A line with neither (or without a
   `path_id`) is skipped with a warning, as a line without `building_id` was.
4. **`DEMAND_SITE_ID` stays required.** It is the fallback for unsited orders
   and the only attribution for task completions. Its meaning in the
   configuration docs becomes "the fallback site".
5. **No replay is forced.** Rows already written keep their location. The
   history is re-attributed only by replaying the topic under a new consumer
   group, exactly as ADR 0004 §4 already describes for a changed
   `DEMAND_SITE_ID`.

## Consequences

- `GET /demand?location=<site>` and plans that default `assigned_demand` from
  orders now see the orders of `<site>` as order-management sited them. A plan
  for `DEMAND_SITE_ID` no longer counts orders that order-management placed at
  another site, so its demand can drop.
- The hot spot "order events carry no site" is closed for orders. It remains
  open for task completions (ADR 0017) and for orders that carry no site.
  Cancellations are still not netted (ADR 0004 §3).
- No new CloudEvents type, extension attribute or topic, so no CloudEvents ADR
  catalogue change. The consumer contract test asserts each golden example lands
  at its own `site_id`, or at the configured site when it carries none.
