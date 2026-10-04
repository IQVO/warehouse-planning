-- 0006_order_demand.up.sql: the expected-demand read model fed by
-- order-management's published order events (docs/adr/0004-demand-ingestion-
-- from-order-management.md). Purely additive.
--
-- order_demand: ONE row per order-management order id, last-writer-wins on
-- the CloudEvents `time` (as_of). order-management carries no fulfillment
-- site on its events, so `location` is the ONE configured site
-- (DEMAND_SITE_ID) the consumer attributes every order to. promise_at is
-- the order's promise cutoff (promise_date); an order is demand in a window
-- [start, end) when start <= promise_at < end. released_lines is the number
-- of lines the latest allocation pass released: informational, NOT units
-- (the events carry no quantities).
CREATE TABLE order_demand (
    order_id       TEXT        PRIMARY KEY,
    location       TEXT        NOT NULL,
    promise_at     TIMESTAMPTZ NOT NULL,
    released_lines INTEGER     NOT NULL CHECK (released_lines >= 0),
    as_of          TIMESTAMPTZ NOT NULL
);

-- The only read: orders of a site by promise cutoff.
CREATE INDEX idx_order_demand_location_promise ON order_demand (location, promise_at);

-- capacity_plans.demand_source: where the plan's assigned_demand came from,
-- 'request' (the caller stated it; every plan created before this
-- migration) or 'orders' (defaulted from order_demand). Informational: no
-- published event carries it.
ALTER TABLE capacity_plans
    ADD COLUMN demand_source TEXT NOT NULL DEFAULT 'request'
    CHECK (demand_source IN ('request', 'orders'));
