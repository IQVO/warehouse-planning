-- 0008_capacity_plan_site_id.up.sql: additive canonical site id on capacity
-- plans (network rollout). site_id references facility-layout's Site
-- site_code (its SiteCapabilityChanged event publishes site_code). It is a
-- REQUIRED planning fact for newly created plans (the REST command API and
-- the domain reject a blank one) and is NEVER inferred from warehouse_id or
-- location. Existing rows keep '' (a plan stored before this migration
-- publishes an empty site_id; the CapacityPlanPublished v1 payload omits it).
ALTER TABLE capacity_plans
    ADD COLUMN site_id TEXT NOT NULL DEFAULT '';
