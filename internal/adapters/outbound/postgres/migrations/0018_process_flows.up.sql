-- 0018_process_flows.up.sql: the local copy of process-path-management's
-- process flows (docs/adr/0025). Purely additive.
--
-- process_flows: ONE row per flow id, fed only by its ProcessFlowDefined /
-- ProcessFlowRevised / ProcessFlowRetired integration events
-- (warehouse.process-path-management.events). Every event is the FULL state of
-- the flow and carries the aggregate version, so a row is replaced only by a
-- message with a GREATER version (the guard is in the UPSERT's WHERE): a
-- redelivery, the producer's startup republish at the current version and a
-- reordered older message change nothing. A retirement is stored as status
-- RETIRED (a RETIRED flow does not resolve a path id; the locally declared path
-- of that id, if any, is used).
--
-- * stages: the ordered array [{"path_id": ..., "process_type": ...}]; the array
--   order IS the flow's order. process_type is one of the closed set below.
-- * updated_at: the CloudEvents `time` of the event that carried the state.
CREATE TABLE process_flows (
    flow_id    TEXT        PRIMARY KEY CHECK (flow_id <> ''),
    name       TEXT        NOT NULL DEFAULT '',
    stages     JSONB       NOT NULL CHECK (
        jsonb_typeof(stages) = 'array' AND jsonb_array_length(stages) > 0
    ),
    status     TEXT        NOT NULL CHECK (status IN ('ACTIVE', 'RETIRED')),
    version    BIGINT      NOT NULL CHECK (version >= 1),
    updated_at TIMESTAMPTZ NOT NULL
);

-- The reads: one ACTIVE flow by id (the primary key) and the ACTIVE flows ordered
-- by id, one keyset page at a time.
CREATE INDEX idx_process_flows_active ON process_flows (flow_id) WHERE status = 'ACTIVE';
