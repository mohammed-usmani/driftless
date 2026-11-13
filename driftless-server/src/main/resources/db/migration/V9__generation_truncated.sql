-- the version of the last TRUNCATE applied to a generation: any change older than it is stale, even for a row
-- with no row_state yet (truncate records have no key, so they can overtake older changes on other partitions)
ALTER TABLE generation ADD COLUMN truncated_major bigint NOT NULL DEFAULT 0;
ALTER TABLE generation ADD COLUMN truncated_minor bigint NOT NULL DEFAULT 0;
