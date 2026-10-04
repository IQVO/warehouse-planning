-- 0003_capacity_plan_and_outbox.up.sql: Phase 4 persistence for the
-- CapacityPlan aggregate and the transactional outbox.

-- capacity_plans: one row per CapacityPlan. All derived fields
-- (path_capacity, bottleneck_step, capacity_over_window, shortage) are
-- stored as computed at creation, so reading a plan never needs the
-- ProcessCapacity read models it was computed from.
CREATE TABLE capacity_plans (
    id                   TEXT             PRIMARY KEY,   -- uuid string
    warehouse_id         TEXT             NOT NULL,
    location             TEXT             NOT NULL,
    window_start         TIMESTAMPTZ      NOT NULL,
    window_end           TIMESTAMPTZ      NOT NULL,
    process_path_id      TEXT             NOT NULL,
    assigned_demand      DOUBLE PRECISION NOT NULL CHECK (assigned_demand >= 0),  -- orders
    path_capacity        DOUBLE PRECISION NOT NULL,                               -- ORDER per HOUR
    bottleneck_step      TEXT             NOT NULL,
    capacity_over_window DOUBLE PRECISION NOT NULL,                               -- orders
    shortage             DOUBLE PRECISION NOT NULL CHECK (shortage >= 0),         -- orders
    status               TEXT             NOT NULL CHECK (status IN ('DRAFT', 'PUBLISHED')),
    created_at           TIMESTAMPTZ      NOT NULL,
    published_at         TIMESTAMPTZ,
    CHECK (window_end > window_start)
);

CREATE INDEX idx_capacity_plans_warehouse_window ON capacity_plans (warehouse_id, window_start);

-- outbox_events: transactional outbox (mirrors workforce-management's
-- outbox_events shape). One row per already-encoded Kafka message,
-- inserted in the SAME transaction as the aggregate (ports.UnitOfWork). A
-- relay goroutine drains unpublished rows to Kafka.
--
--   event_id    the CloudEvents `id`, minted ONCE at encode time and
--               persisted: a relay retry republishes the same bytes, so
--               consumers can dedupe on it.
--   event_type  the FULL CloudEvents `type`
--               (com.warehouse.wes.warehouse-planning.capacityplan.<Event>).
--   subject     the CloudEvents `subject` (the aggregate id).
--   key         the Kafka message key (the aggregate id).
--   dataschema  the CloudEvents `dataschema` URN.
--   value       the encoded structured-mode CloudEvents JSON bytes.
--   headers     [{"key":..,"value":..}] Kafka headers (content-type).
CREATE TABLE outbox_events (
    id           BIGSERIAL   PRIMARY KEY,
    event_id     TEXT        NOT NULL UNIQUE,
    topic        TEXT        NOT NULL,
    event_type   TEXT        NOT NULL,
    subject      TEXT        NOT NULL,
    key          BYTEA,
    dataschema   TEXT        NOT NULL,
    value        BYTEA       NOT NULL,
    headers      JSONB       NOT NULL DEFAULT '[]',
    created_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    published_at TIMESTAMPTZ,
    attempts     INTEGER     NOT NULL DEFAULT 0,
    last_error   TEXT
);

-- The relay only ever scans the unpublished tail; keep that scan tiny.
CREATE INDEX idx_outbox_events_unpublished ON outbox_events (id) WHERE published_at IS NULL;
