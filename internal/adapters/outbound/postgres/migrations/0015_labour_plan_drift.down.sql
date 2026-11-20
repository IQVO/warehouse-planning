-- 0015_labour_plan_drift.down.sql: drops the committed side of the labour
-- plan drift (docs/adr/0023 decision 3).
DROP TABLE IF EXISTS labour_plan_commitments;
