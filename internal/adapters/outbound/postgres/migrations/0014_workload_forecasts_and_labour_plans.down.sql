-- 0014_workload_forecasts_and_labour_plans.down.sql: drops the stored
-- WorkloadForecast and LabourPlan aggregates (docs/adr/0023).
DROP TABLE IF EXISTS labour_plans;
DROP TABLE IF EXISTS workload_forecasts;
