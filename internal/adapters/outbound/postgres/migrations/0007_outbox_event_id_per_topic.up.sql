-- 0007_outbox_event_id_per_topic.up.sql: ADR 0005 (analytics read side).
--
-- One domain event now becomes TWO outbox rows in the same transaction: one
-- on the integration topic and one on the analytics topic. Both carry the
-- SAME CloudEvents `id` (minted once per occurrence), so `event_id` can no
-- longer be unique on its own: the identity of a row is (event_id, topic).
-- The pair stays unique, so an event can still never be enqueued twice for
-- the same topic. The projector dedupes on the CloudEvents id (one topic),
-- the integration consumers dedupe on it too (the other topic).
ALTER TABLE outbox_events DROP CONSTRAINT outbox_events_event_id_key;
ALTER TABLE outbox_events ADD CONSTRAINT outbox_events_event_id_topic_key UNIQUE (event_id, topic);
