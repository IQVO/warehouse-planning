-- 0005_station_standards_and_cleanup.up.sql: read-time station-capacity
-- composition (ADR 0002, docs/adr/0002-station-capacity-composition.md).
--
-- 1. station_standards: the operator-declared throughput of ONE station of a
--    process at a site, e.g. (SIM1, PACK) = 180 PACKAGE per 3600 s. A station
--    COUNT (tallied from facility-layout in location_slot_tally) has no
--    throughput of its own; count x this standard is composed with the
--    registered constraints AT READ TIME and never stored. period_seconds is
--    the rate's period (3600 = per hour), like process_capacity_constraint.
CREATE TABLE station_standards (
    location       TEXT             NOT NULL,
    process_type   TEXT             NOT NULL,
    quantity       DOUBLE PRECISION NOT NULL CHECK (quantity > 0),
    unit           TEXT             NOT NULL,
    period_seconds DOUBLE PRECISION NOT NULL CHECK (period_seconds > 0),
    updated_at     TIMESTAMPTZ      NOT NULL DEFAULT now(),
    PRIMARY KEY (location, process_type)
);

-- 2. capacity_plans gains the composition outcome (informational read-model
--    fields; no published event carries them): the constraint type binding the
--    bottleneck step and the warnings raised while composing. Existing plans
--    keep '' / an empty array.
ALTER TABLE capacity_plans
    ADD COLUMN bottleneck_constraint TEXT   NOT NULL DEFAULT '',
    ADD COLUMN warnings              TEXT[] NOT NULL DEFAULT '{}';

-- 3. Cleanup of legacy DERIVED rows. Until now the facility-layout consumer
--    materialized its tallies as ProcessCapacity constraints: a sentinel
--    process_type 'STORAGE' (location '<zoneId>:<locationType>') for storage
--    positions, and STATION constraints in unit LINE on a 'standing window'
--    [1970-01-01, 2070-01-01) keyed by ZONE for work-center stations. Both
--    are derived data that nothing reads (they never combined with LABOR, and
--    a position/station COUNT is not a throughput); the tally in
--    location_slot_tally stays the source of truth and the consumer now only
--    maintains it. process_capacity_constraint rows are removed by the
--    ON DELETE CASCADE foreign key. Real ProcessCapacity rows (labor, ...)
--    never have a 1970 window start or the STORAGE sentinel type.
DELETE FROM process_capacity
WHERE process_type = 'STORAGE'
   OR window_start = TIMESTAMPTZ '1970-01-01 00:00:00+00';
