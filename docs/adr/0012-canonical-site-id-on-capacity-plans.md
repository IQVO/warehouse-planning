# ADR 0012: Canonical site_id on CapacityPlan and CapacityPlanPublished

## Status

Accepted (2026-10-06).

## Context

The fleet is gaining a network scope: several warehouses, each site able to
act as a transfer origin/destination. `facility-layout` owns the Site
aggregate and, since its `SiteCapabilityChanged` event, publishes each site's
`site_code` — the one canonical identifier of a site across contexts.

A `CapacityPlan`, however, is scoped only by `warehouse_id` + `location`.
`warehouse_id` is a free-text label (`WH-1`), and `location` is a planning
location (the ProcessCapacity lookup key, e.g. `PATH-ZONE-A` or a site code
when the labor consumer's `building_id` is used). Neither is a site
reference, and deriving one — by prefix-parsing `location` or assuming
`warehouse_id` names a site — would be exactly the kind of inference ADR 0002
banned for station composition ("neither derives anything it cannot derive
honestly").

Downstream, `order-management` already attributes consumed plans to a
configured `PLANNED_CAPACITY_SITE_ID` because the events carry no site; a
per-plan site id is the honest fix.

## Decision

1. `CapacityPlan` carries a canonical `site_id`: a facility-layout Site
   `site_code` (the same identifier `SiteCapabilityChanged` publishes). It is
   a STATED planning fact on `POST /capacity-plans` (and the
   `create_capacity_plan` MCP tool): REQUIRED, and never inferred from
   `warehouse_id` or `location`. A blank or absent one is rejected with 422
   `missing-required-field` (`capacityplan.ErrRequiredField`).
2. Persistence is additive: migration `0008` adds
   `capacity_plans.site_id TEXT NOT NULL DEFAULT ''`. Existing rows keep `''`
   and read back as plans with an empty site id (the REST/MCP response
   exposes `site_id` additively, empty for those plans).
3. `CapacityPlanPublished`'s v1 payload gains `site_id` ADDITIVELY. The
   `type` (`com.warehouse.wes.warehouse-planning.capacityplan.CapacityPlanPublished`),
   `dataschema`
   (`urn:warehouse:warehouse-planning:events:CapacityPlanPublished:v1`),
   `source` (`/warehouse/warehouse-planning`) and topic are UNCHANGED — an
   additive optional field is not a breaking payload change, so no `.v2`.
   The producer emits `site_id` with `omitempty`: a plan stored before
   migration `0008` publishes bytes byte-identical to the pre-site_id shape,
   and a legacy v1 payload without `site_id` still decodes (unknown-field
   tolerance). The analytics payload inherits the field the same way
   `location` does (it embeds the integration payload).
4. Only `CapacityPlanPublished` carries `site_id` on the wire. The other
   three events keep their exact shapes: consumers key network scope on the
   plan's publication; `CapacityPlanCreated` is a DRAFT-lifecycle fact.

## Consequences

- Producers and consumers can ship in any order: the field is additive on
  the wire and omitted-when-empty, so a mixed-version fleet is safe.
- A consumer reading an old plan's publication sees no `site_id` and must
  treat the site as unknown — never derive it. `order-management`'s
  configured-site fallback remains the answer for those plans.
- Migration `0008` is online-safe: `ADD COLUMN ... NOT NULL DEFAULT ''`
  takes a metadata-only lock on Postgres.
- The REST/MCP request surface is a BREAKING additive change for clients
  that create plans: they must now send `site_id`. That is deliberate —
  a silently-defaulted site id would reintroduce the inference this ADR
  removes. The web console (`web/src`) ships in the same rollout window
  and is updated to require the field.

## Alternatives considered

- **Infer the site from `location`'s zone-id prefix** (the ADR 0002
  site-equals-first-dash-segment rule): rejected — that rule describes how
  facility zone ids are CODED, not a license to derive planning scope; a
  planning location is not always a zone-prefixed string.
- **Publish a `.v2` dataschema**: rejected — additive optional fields under
  an unchanged `dataschema` are the fleet's established additive pattern
  (`bottleneck_constraint` on the analytics stream, `demand_source` on the
  REST response); a v2 would force coordinated consumer migration for no
  compatibility gain.
- **Key plans by site instead of warehouse**: rejected — changes the
  aggregate's natural key and every consumer's read model for zero
  additional information over the additive column.
