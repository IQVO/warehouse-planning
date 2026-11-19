-- 0014_workload_forecasts_and_labour_plans.up.sql: the WorkloadForecast and
-- LabourPlan aggregates, moved here from wes-work-planning (docs/adr/0023).
-- Purely additive.
--
-- Both tables carry the aggregate's optimistic-concurrency version: the
-- repositories INSERT at version 1 (ON CONFLICT DO NOTHING, so a duplicate is
-- a version conflict that leaves an enclosing unit of work usable) and UPDATE
-- a later version only WHERE the stored version is the new one minus 1.
-- Rows are rehydrated through the domain's Rehydrate, which re-checks every
-- invariant; the CHECKs below mirror the cheap ones so a hand-written row
-- cannot get past the database either.

-- workload_forecasts: ONE row per (site_id, path_id). buckets is the whole
-- receipt, [{"cpt": RFC 3339 with nanoseconds, "quantity": units}, ...] in
-- receipt order, at least one bucket; a new receipt replaces it whole.
-- received_at is when the current buckets were received.
CREATE TABLE workload_forecasts (
    site_id     TEXT        NOT NULL CHECK (site_id <> ''),
    path_id     TEXT        NOT NULL CHECK (path_id <> ''),
    buckets     JSONB       NOT NULL CHECK (
        CASE WHEN jsonb_typeof(buckets) = 'array' THEN jsonb_array_length(buckets) >= 1 ELSE false END
    ),
    received_at TIMESTAMPTZ NOT NULL,
    version     BIGINT      NOT NULL CHECK (version >= 1),
    PRIMARY KEY (site_id, path_id)
);

-- labour_plans: ONE row per (site_id, shift_id, path_id), every field of
-- labourplan.Spec. path_id is COLLATE "C" so the list read (one site and
-- shift, ordered by path_id, after a path-id cursor) pages in byte order,
-- whatever the database's default collation. INTEGER is the domain's count
-- bound (labourplan.MaxCount = 2147483647). shift_start is optional (deriving
-- heads from the forecast needs it); heads_source says where planned_heads
-- came from. The rate and the hours are finite and above zero ('NaN' and
-- 'Infinity' compare above every number in Postgres, hence the upper bound).
CREATE TABLE labour_plans (
    site_id             TEXT             NOT NULL CHECK (site_id <> ''),
    shift_id            TEXT             NOT NULL CHECK (shift_id <> ''),
    path_id             TEXT COLLATE "C" NOT NULL CHECK (path_id <> ''),
    planned_heads       INTEGER          NOT NULL CHECK (planned_heads >= 0),
    rate_units_per_hour DOUBLE PRECISION NOT NULL CHECK (rate_units_per_hour > 0 AND rate_units_per_hour < 'Infinity'),
    hours               DOUBLE PRECISION NOT NULL CHECK (hours > 0 AND hours < 'Infinity'),
    installed_stations  INTEGER          NOT NULL CHECK (installed_stations >= 0),
    shift_start         TIMESTAMPTZ,
    heads_source        TEXT             NOT NULL CHECK (heads_source IN ('request', 'forecast')),
    version             BIGINT           NOT NULL CHECK (version >= 1),
    PRIMARY KEY (site_id, shift_id, path_id),
    CHECK (planned_heads <= installed_stations)
);
