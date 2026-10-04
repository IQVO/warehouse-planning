-- 0007_outbox_event_id_per_topic.down.sql: restores UNIQUE (event_id).
-- Rows on the analytics topic share their event_id with the integration row
-- of the same occurrence, so they are removed first (the analytics stream is
-- a derived copy; the integration rows are the source of record).
DELETE FROM outbox_events WHERE topic = 'warehouse.warehouse-planning.analytics';
ALTER TABLE outbox_events DROP CONSTRAINT outbox_events_event_id_topic_key;
ALTER TABLE outbox_events ADD CONSTRAINT outbox_events_event_id_key UNIQUE (event_id);
