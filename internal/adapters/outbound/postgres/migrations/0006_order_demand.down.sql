-- 0006_order_demand.down.sql: drops exactly what the up migration added.
-- Plans created with demand_source = 'orders' keep their assigned_demand;
-- only the provenance label is lost.
ALTER TABLE capacity_plans DROP COLUMN IF EXISTS demand_source;
DROP INDEX IF EXISTS idx_order_demand_location_promise;
DROP TABLE IF EXISTS order_demand;
