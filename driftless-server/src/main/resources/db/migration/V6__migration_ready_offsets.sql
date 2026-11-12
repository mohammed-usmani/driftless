-- Change-topic end offsets captured when a migration's snapshot finished; the generation is READY once
-- the consumer has committed past all of them.
ALTER TABLE generation ADD COLUMN ready_offsets jsonb;
