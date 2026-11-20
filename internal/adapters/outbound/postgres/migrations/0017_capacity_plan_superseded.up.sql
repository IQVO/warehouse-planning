-- 0017_capacity_plan_superseded.up.sql: publishing a capacity plan
-- supersedes the plan published before it for the same site, location, path
-- and window (docs/adr/0024 decision 3).
--
-- status gains the terminal 'SUPERSEDED'. The inline CHECK of 0003 is named
-- capacity_plans_status_check by Postgres; it is replaced, not stacked.
ALTER TABLE capacity_plans DROP CONSTRAINT capacity_plans_status_check;
ALTER TABLE capacity_plans
    ADD CONSTRAINT capacity_plans_status_check
    CHECK (status IN ('DRAFT', 'PUBLISHED', 'SUPERSEDED'));

-- superseded_by: the id of the plan whose publication superseded this one.
-- Set exactly when the status is SUPERSEDED; NULL for every other plan and
-- for every plan stored before this migration.
ALTER TABLE capacity_plans
    ADD COLUMN superseded_by TEXT
    CHECK (superseded_by IS NULL OR superseded_by <> '');
ALTER TABLE capacity_plans
    ADD CONSTRAINT capacity_plans_superseded_iff_status_check
    CHECK ((status = 'SUPERSEDED') = (superseded_by IS NOT NULL));

-- Backfill: the deployed databases may already hold several PUBLISHED plans
-- of one key (site_id, location, process_path_id, window_start, window_end),
-- and the index below would then fail to build. Every PUBLISHED plan except
-- the newest of its key (by published_at, then id) becomes SUPERSEDED by that
-- newest plan. This is a data repair: NO event is published for it (no
-- outbox row is written), so consumers that kept every published plan keep
-- them until a later publish of the key supersedes the newest one.
WITH ranked AS (
    SELECT id,
           first_value(id) OVER key_newest_first AS newest_id,
           row_number()    OVER key_newest_first AS rank_in_key
    FROM capacity_plans
    WHERE status = 'PUBLISHED'
    WINDOW key_newest_first AS (
        PARTITION BY site_id, location, process_path_id, window_start, window_end
        ORDER BY published_at DESC NULLS LAST, id DESC
    )
)
UPDATE capacity_plans AS p
SET status = 'SUPERSEDED', superseded_by = ranked.newest_id
FROM ranked
WHERE p.id = ranked.id AND ranked.rank_in_key > 1;

-- One PUBLISHED plan per key is a database fact: of two concurrent publishes
-- of one key that both saw no published plan, the second to commit fails here
-- (23505), and the use case reruns its unit, which then supersedes the first.
CREATE UNIQUE INDEX capacity_plans_one_published_per_key
    ON capacity_plans (site_id, location, process_path_id, window_start, window_end)
    WHERE status = 'PUBLISHED';
