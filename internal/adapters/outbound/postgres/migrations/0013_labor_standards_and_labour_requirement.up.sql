-- 0013_labor_standards_and_labour_requirement.up.sql: the local copy of
-- labor-performance's engineered labour standards, and the labour each step of a
-- capacity plan requires (docs/adr/0022). Purely additive.
--
-- labor_standards: ONE row per labor-performance standard id, fed only by its
-- LaborStandardDefined / LaborStandardRevised integration events
-- (warehouse.labor-performance.events). The copy stores FACTS, not a current
-- value: a revision in labor-performance closes the prior record and opens a
-- new one under a new id, so the standard that applies to a plan is resolved
-- at READ time by laborstandard.Resolve (site-specific over site-less, then the
-- latest effective_from, then the latest as_of). A redelivered or replayed id
-- changes nothing (INSERT ... ON CONFLICT DO NOTHING).
--
-- * handling_unit: '' for a task-type-only standard (the only kind a plan
--   uses), else EACH / CASE / PALLET (stored, not used yet).
-- * site_code: '' = every site (labor-performance ADR 0038's optional scope).
-- * as_of: the CloudEvents `time` of the event that carried the record.
CREATE TABLE labor_standards (
    standard_id              TEXT        PRIMARY KEY,
    task_type                TEXT        NOT NULL CHECK (task_type <> ''),
    handling_unit            TEXT        NOT NULL DEFAULT '' CHECK (handling_unit IN ('', 'EACH', 'CASE', 'PALLET')),
    site_code                TEXT        NOT NULL DEFAULT '',
    expected_seconds         BIGINT      NOT NULL CHECK (expected_seconds > 0),
    travel_component_seconds BIGINT      CHECK (travel_component_seconds >= 0),
    effective_from           TIMESTAMPTZ NOT NULL,
    as_of                    TIMESTAMPTZ NOT NULL
);

-- The only read: the task-type-only standards of a task type at a site (or
-- every site) in force at a window start.
CREATE INDEX idx_labor_standards_task_site_effective
    ON labor_standards (task_type, site_code, effective_from) WHERE handling_unit = '';

-- capacity_plans.labour_requirement: per step of the plan's path, the labour its
-- demand requires (process_type, standard_seconds, standard_site_scoped, tasks,
-- required_labour_hours, required_average_heads, known, reason), computed when
-- the plan is created and stored with it, like the path capacity: a later
-- revision of a standard never rewrites a stored plan. Informational: no
-- published event carries it. Plans stored before this migration read '[]'.
ALTER TABLE capacity_plans
    ADD COLUMN labour_requirement JSONB NOT NULL DEFAULT '[]'::jsonb;
