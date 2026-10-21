-- 0010_observed_capacity.down.sql: undo 0010. The read model is rebuilt from the
-- topic by a fresh consumer group.
DROP TABLE observed_capacity;
