-- 0013_labor_standards_and_labour_requirement.down.sql: drops the labour
-- requirement stored with the capacity plans and the local copy of
-- labor-performance's labour standards (docs/adr/0022).
ALTER TABLE capacity_plans DROP COLUMN IF EXISTS labour_requirement;
DROP TABLE IF EXISTS labor_standards;
