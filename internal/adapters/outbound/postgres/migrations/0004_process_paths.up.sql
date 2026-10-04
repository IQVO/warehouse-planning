-- 0004_process_paths.up.sql: persistence for the ProcessPath read model
-- (previously in-memory only, so a pod restart lost every declared path).
--
-- steps is a text[] column: Postgres arrays preserve element order, which
-- is exactly the semantics a ProcessPath needs (Pick -> Rebin -> Pack).
-- Saving an existing id replaces name and steps wholesale, mirroring the
-- in-memory repository's upsert (a changed path is a new read model, never
-- an in-place edit).
CREATE TABLE process_paths (
    id    TEXT   PRIMARY KEY,
    name  TEXT   NOT NULL,
    steps TEXT[] NOT NULL CHECK (cardinality(steps) > 0)
);
