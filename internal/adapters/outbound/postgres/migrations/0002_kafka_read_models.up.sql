-- 0002_kafka_read_models.up.sql: Phase 3 local read models for the Kafka
-- consumers (labor and storage/station capacity ingestion). These tables
-- are owned entirely by this context -- they are NOT shared aggregates,
-- just bookkeeping the consumers need to stay idempotent and to know what
-- to decrement on a LocationSlotDecommissioned event.

-- processed_events: idempotency guard shared by every inbound Kafka
-- consumer in this service. (consumer, event_id) is claimed with an
-- INSERT ... ON CONFLICT DO NOTHING *before* any side effect is applied;
-- zero rows affected means this exact event was already handled by this
-- consumer, so the caller skips reprocessing it. This is what makes a
-- redelivered ShiftPlanCommitted/LocationSlotRegistered/Decommissioned
-- message a no-op rather than a double-counted increment.
CREATE TABLE processed_events (
    consumer     TEXT        NOT NULL,
    event_id     TEXT        NOT NULL,
    processed_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (consumer, event_id)
);

-- location_slot_registration: remembers which tally bucket(s) a given
-- facility-layout locationCode contributed to, so a later
-- LocationSlotDecommissioned (which carries only locationCode) knows
-- exactly what to decrement without needing the original Registered
-- payload again. Deleted when the slot is decommissioned -- a
-- decommission for a locationCode no longer present here is logged as an
-- untracked slot and is a no-op (never a crash, never a negative count).
CREATE TABLE location_slot_registration (
    location_code TEXT   NOT NULL PRIMARY KEY,
    zone_id        TEXT  NOT NULL,
    -- tally_type is 'LOCATION' (role=Storage) or 'STATION' (role=WorkCenter).
    tally_type     TEXT  NOT NULL,
    -- tally_keys: locationType (one entry) for LOCATION, or the
    -- uppercased activities list (one or more entries) for STATION.
    tally_keys     TEXT[] NOT NULL
);

-- location_slot_tally: the running count this phase registers as a
-- CapacityConstraint's quantity. Keyed by (zone_id, tally_type, tally_key)
-- -- e.g. (ZONE-A, LOCATION, BULK) or (ZONE-A, STATION, PACK). count is
-- floored at 0 by the repository, never allowed to go negative.
CREATE TABLE location_slot_tally (
    zone_id    TEXT    NOT NULL,
    tally_type TEXT    NOT NULL,
    tally_key  TEXT    NOT NULL,
    count      INTEGER NOT NULL DEFAULT 0,
    PRIMARY KEY (zone_id, tally_type, tally_key)
);
