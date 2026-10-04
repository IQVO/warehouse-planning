-- warehouse-planning analytics read model (ADR 0005).
--
-- This is the ANALYTICAL database (warehouse_planning_analytics), separate
-- from the OLTP database. It is written only by cmd/planning-projector and
-- read (read-only) by cmd/planning-reports. Everything here is a projection
-- derived from the analytics event stream, not a source of truth.

-- Idempotency: every applied CloudEvents id is recorded here exactly once,
-- in the SAME transaction as its effect on plan_facts. occurred_at is the
-- event's CloudEvents `time`.
CREATE TABLE analytics_processed_events (
    event_id    TEXT        PRIMARY KEY,
    event_type  TEXT        NOT NULL,
    occurred_at TIMESTAMPTZ NOT NULL,
    applied_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- plan_facts: one row per CapacityPlan, assembled from its four events.
-- A column an event does not carry is left as it was (the projection only
-- overwrites what the event states), so the row converges whatever order
-- the events arrive in. created_at is NULL for a plan created before the
-- analytics stream existed; published_at is NULL until it is published.
--
--   bottleneck_step     the step with the lowest capacity (PICK, PACK, ...)
--   binding_constraint  the constraint type binding that step (LABOR,
--                       STATION, ...), carried by CapacityPlanPublished
--                       only; '' when the plan predates it
--   shortage            orders the window's capacity falls short of demand
CREATE TABLE plan_facts (
    plan_id            TEXT             PRIMARY KEY,
    warehouse_id       TEXT             NOT NULL,
    location           TEXT             NOT NULL,
    bottleneck_step    TEXT             NOT NULL DEFAULT '',
    binding_constraint TEXT             NOT NULL DEFAULT '',
    shortage           DOUBLE PRECISION NOT NULL DEFAULT 0 CHECK (shortage >= 0),
    created_at         TIMESTAMPTZ,
    published_at       TIMESTAMPTZ
);

-- Every report filters on one of these two instants.
CREATE INDEX idx_plan_facts_published_at ON plan_facts (published_at) WHERE published_at IS NOT NULL;
CREATE INDEX idx_plan_facts_created_at   ON plan_facts (created_at)   WHERE created_at IS NOT NULL;
