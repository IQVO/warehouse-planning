-- 0017_capacity_plan_superseded.down.sql: reverts docs/adr/0024 decision 3.
-- A superseded plan reads back as PUBLISHED (the only published status 0016
-- knows) and the record of what superseded it is dropped with its column.
-- No event is published either way.
DROP INDEX IF EXISTS capacity_plans_one_published_per_key;
ALTER TABLE capacity_plans DROP CONSTRAINT IF EXISTS capacity_plans_superseded_iff_status_check;
UPDATE capacity_plans SET status = 'PUBLISHED' WHERE status = 'SUPERSEDED';
ALTER TABLE capacity_plans DROP COLUMN IF EXISTS superseded_by;
ALTER TABLE capacity_plans DROP CONSTRAINT capacity_plans_status_check;
ALTER TABLE capacity_plans
    ADD CONSTRAINT capacity_plans_status_check
    CHECK (status IN ('DRAFT', 'PUBLISHED'));
