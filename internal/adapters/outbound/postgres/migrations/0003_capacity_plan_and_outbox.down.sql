-- 0003_capacity_plan_and_outbox.down.sql
DROP INDEX IF EXISTS idx_outbox_events_unpublished;
DROP TABLE IF EXISTS outbox_events;
DROP INDEX IF EXISTS idx_capacity_plans_warehouse_window;
DROP TABLE IF EXISTS capacity_plans;
