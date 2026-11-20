-- 0019_drop_process_paths.down.sql: recreate the table exactly as
-- 0004_process_paths.up.sql created it (the declaration code this migration's
-- up removed is gone, so the table stays empty unless a downgrade also
-- restores that code).
CREATE TABLE process_paths (
    id    TEXT   PRIMARY KEY,
    name  TEXT   NOT NULL,
    steps TEXT[] NOT NULL CHECK (cardinality(steps) > 0)
);
