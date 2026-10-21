-- 0009_inbound_labour_demand.down.sql: undo 0009. Dropping the plan columns loses
-- the inbound figures stored with existing plans (informational only).
ALTER TABLE capacity_plans
    DROP COLUMN inbound_standard_missing,
    DROP COLUMN inbound_labour_minutes,
    DROP COLUMN inbound_appointments;

DROP TABLE inbound_labour_standards;
DROP TABLE inbound_appointments;
