-- 0016_forecast_demand_and_scenarios.up.sql: forecast demand and plan
-- scenarios (docs/adr/0024 decisions 1 and 2). Purely additive for every row
-- already stored: no existing value changes.
--
-- demand_source gains 'forecast': the plan's assigned_demand (orders) was
-- derived from the workload forecast of (site_id, forecast_path_id), the units
-- whose bucket cpt is in the window over units_per_order, rounded up. The
-- inline CHECK of 0006 is named capacity_plans_demand_source_check by
-- Postgres; it is replaced, not stacked.
ALTER TABLE capacity_plans DROP CONSTRAINT capacity_plans_demand_source_check;
ALTER TABLE capacity_plans
    ADD CONSTRAINT capacity_plans_demand_source_check
    CHECK (demand_source IN ('request', 'orders', 'forecast'));

-- forecast_path_id: the canonical path code of the forecast a forecast plan
-- read (it need not be the plan's process_path_id). NULL for every other plan
-- and for every plan stored before this migration; never blank.
ALTER TABLE capacity_plans
    ADD COLUMN forecast_path_id TEXT
    CHECK (forecast_path_id IS NULL OR forecast_path_id <> '');

-- scenario: the optional what-if label of a plan (decision 2). Several DRAFT
-- plans of one (site_id, process_path_id, window) are scenarios. NULL is no
-- label; a label is 1 to 64 characters (capacityplan.MaxScenarioLength).
ALTER TABLE capacity_plans
    ADD COLUMN scenario TEXT
    CHECK (scenario IS NULL OR char_length(scenario) BETWEEN 1 AND 64);

-- The additive list filters (site_id, path_id, window) and the comparison
-- read look plans up by site, path and window, newest first.
CREATE INDEX capacity_plans_site_path_window_idx
    ON capacity_plans (site_id, process_path_id, window_start, window_end, created_at DESC);
