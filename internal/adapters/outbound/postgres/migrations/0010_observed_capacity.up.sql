-- 0010_observed_capacity.up.sql: the observed-throughput read model fed by
-- fulfillment-execution's TaskCompleted events (docs/adr/0017-inbound-labour-
-- demand-and-observed-capacity-feedback.md). Purely additive.
--
-- observed_capacity: completed tasks aggregated per (site, process step, minute).
-- The event carries no site and no process path, so `location` is the ONE
-- configured site (DEMAND_SITE_ID) and `process_type` is the event's task_type
-- (PICK, PACK, ...), upper-cased. bucket_start is the event `time` truncated to
-- the minute (UTC); a window [start, end) reads the buckets with
-- start <= bucket_start < end. The consumer's processed-event claim and the
-- additive upsert are ONE transaction, so a redelivered event id adds nothing.
-- This table is ADVISORY input for the plan variance read: nothing here changes
-- a planned capacity.
CREATE TABLE observed_capacity (
    location               TEXT        NOT NULL,
    process_type           TEXT        NOT NULL,
    bucket_start           TIMESTAMPTZ NOT NULL,
    tasks_completed        BIGINT      NOT NULL CHECK (tasks_completed > 0),
    duration_seconds_total BIGINT      NOT NULL DEFAULT 0 CHECK (duration_seconds_total >= 0),
    tasks_with_duration    BIGINT      NOT NULL DEFAULT 0 CHECK (tasks_with_duration >= 0),
    as_of                  TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (location, process_type, bucket_start)
);
