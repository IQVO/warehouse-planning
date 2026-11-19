-- 0015_labour_plan_drift.up.sql: the committed side of the labour plan drift
-- (docs/adr/0023 decision 3). Purely additive: the LABOR constraint tables
-- and labour_plans are not changed.
--
-- labour_plan_commitments: ONE row per (site_id, shift_id, path_id), the
-- heads workforce-management COMMITTED for the key (its ShiftPlanCommitted,
-- read by the labor capacity consumer) and when that commitment was observed
-- (the CloudEvents time). last_drift_heads is the last drift RECORDED for the
-- key (committed - planned heads, zero included); NULL until a labour plan
-- existed when a drift could be computed. A drift is raised only when it is
-- non-zero and differs from last_drift_heads, so a repeat of the same drift
-- is never raised twice while a drift that clears and returns is.
-- INTEGER is the domain's count bound (labourplan.MaxCount = 2147483647).
CREATE TABLE labour_plan_commitments (
    site_id          TEXT        NOT NULL CHECK (site_id <> ''),
    shift_id         TEXT        NOT NULL CHECK (shift_id <> ''),
    path_id          TEXT        NOT NULL CHECK (path_id <> ''),
    committed_heads  INTEGER     NOT NULL CHECK (committed_heads >= 0),
    observed_at      TIMESTAMPTZ NOT NULL,
    last_drift_heads INTEGER,
    PRIMARY KEY (site_id, shift_id, path_id)
);
