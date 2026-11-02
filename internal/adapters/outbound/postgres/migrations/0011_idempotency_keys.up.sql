-- 0011_idempotency_keys.up.sql: REST idempotency (fleet REST API guideline,
-- docs/adr/0019). One row per Idempotency-Key, looked up by key alone and
-- written in the same transaction as the aggregate change and its outbox rows.
CREATE TABLE idempotency_keys (
    key         TEXT        PRIMARY KEY,
    fingerprint TEXT        NOT NULL,
    status      INTEGER     NOT NULL,
    content_type TEXT       NOT NULL DEFAULT '',
    location    TEXT        NOT NULL DEFAULT '',
    etag        TEXT        NOT NULL DEFAULT '',
    body        BYTEA       NOT NULL,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX idx_idempotency_keys_created_at ON idempotency_keys (created_at);
