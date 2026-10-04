-- 0005_station_standards_and_cleanup.down.sql: drops what the up migration
-- added. The legacy derived rows the up migration deleted are NOT restored:
-- they were derived data nothing read, and the tally they were derived from
-- (location_slot_tally) is untouched.
ALTER TABLE capacity_plans
    DROP COLUMN IF EXISTS warnings,
    DROP COLUMN IF EXISTS bottleneck_constraint;
DROP TABLE IF EXISTS station_standards;
