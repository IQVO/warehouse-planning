-- 0008_capacity_plan_site_id.down.sql: drop the additive site id column.
ALTER TABLE capacity_plans
    DROP COLUMN site_id;
