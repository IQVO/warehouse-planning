-- 0018_process_flows.down.sql: drops the local copy of the process flows.
DROP INDEX IF EXISTS idx_process_flows_active;
DROP TABLE IF EXISTS process_flows;
