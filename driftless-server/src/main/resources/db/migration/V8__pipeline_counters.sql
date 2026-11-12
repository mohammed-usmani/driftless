-- Running totals for the ops API; recent_change is trimmed, these are not.
ALTER TABLE pipeline ADD COLUMN chunks_embedded bigint NOT NULL DEFAULT 0;
ALTER TABLE pipeline ADD COLUMN embeds_saved    bigint NOT NULL DEFAULT 0;
