-- 0016_forecast_demand_and_scenarios.down.sql: reverts docs/adr/0024
-- decisions 1 and 2. A plan whose demand came from the forecast keeps its
-- assigned_demand and reads back as 'request' (a stated figure), the only
-- source 0015 knows; the forecast path it read and the scenario labels are
-- dropped with their columns.
DROP INDEX IF EXISTS capacity_plans_site_path_window_idx;
ALTER TABLE capacity_plans DROP COLUMN IF EXISTS scenario;
ALTER TABLE capacity_plans DROP COLUMN IF EXISTS forecast_path_id;
UPDATE capacity_plans SET demand_source = 'request' WHERE demand_source = 'forecast';
ALTER TABLE capacity_plans DROP CONSTRAINT capacity_plans_demand_source_check;
ALTER TABLE capacity_plans
    ADD CONSTRAINT capacity_plans_demand_source_check
    CHECK (demand_source IN ('request', 'orders'));
