-- 0001_process_capacity.up.sql: Phase 1 persistence for the
-- ProcessCapacity aggregate. Two tables: the parent keyed by the
-- aggregate's identity (process_type, location, window_start, window_end)
-- storing its native unit, and the child holding one row per registered
-- constraint, cascade-deleted with its parent.
CREATE TABLE process_capacity (
    process_type  TEXT        NOT NULL,
    location      TEXT        NOT NULL,
    window_start  TIMESTAMPTZ NOT NULL,
    window_end    TIMESTAMPTZ NOT NULL,
    native_unit   TEXT        NOT NULL,
    PRIMARY KEY (process_type, location, window_start, window_end)
);

CREATE TABLE process_capacity_constraint (
    process_type    TEXT             NOT NULL,
    location        TEXT             NOT NULL,
    window_start    TIMESTAMPTZ      NOT NULL,
    window_end      TIMESTAMPTZ      NOT NULL,
    constraint_type TEXT             NOT NULL,
    quantity        DOUBLE PRECISION NOT NULL,
    period_seconds  DOUBLE PRECISION NOT NULL,
    -- ordinal preserves registration order so EffectiveRate's
    -- earliest-registered tie-break stays deterministic after a
    -- save/load round trip.
    ordinal         INTEGER          NOT NULL,
    PRIMARY KEY (process_type, location, window_start, window_end, constraint_type),
    FOREIGN KEY (process_type, location, window_start, window_end)
        REFERENCES process_capacity (process_type, location, window_start, window_end)
        ON DELETE CASCADE
);
