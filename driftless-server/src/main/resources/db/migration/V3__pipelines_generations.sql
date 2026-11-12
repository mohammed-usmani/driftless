CREATE TABLE pipeline (
    name               text        PRIMARY KEY,
    table_name         text        NOT NULL,
    status             text        NOT NULL,
    live_generation_id bigint,
    blocked_reason     text,
    created_at         timestamptz NOT NULL DEFAULT now(),
    updated_at         timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE generation (
    id               bigserial   PRIMARY KEY,
    pipeline         text        NOT NULL REFERENCES pipeline (name),
    number           int         NOT NULL,
    provider         text        NOT NULL,
    model            text        NOT NULL,
    dimensions       int         NOT NULL,
    template         text        NOT NULL,
    max_chars        int         NOT NULL,
    overlap_chars    int         NOT NULL,
    metadata_columns text[]      NOT NULL,
    config_hash      text        NOT NULL,
    status           text        NOT NULL,
    chunk_table      text        NOT NULL,
    created_at       timestamptz NOT NULL DEFAULT now(),
    ready_at         timestamptz,
    live_at          timestamptz,
    dropped_at       timestamptz,
    UNIQUE (pipeline, number)
);
-- at most one LIVE generation per pipeline
CREATE UNIQUE INDEX generation_one_live ON generation (pipeline) WHERE status = 'LIVE';

CREATE TABLE row_state (
    generation_id bigint      NOT NULL REFERENCES generation (id) ON DELETE CASCADE,
    row_key       text        NOT NULL,
    version_major bigint      NOT NULL,
    version_minor bigint      NOT NULL,
    doc_hash      text,
    meta_hash     text,
    chunk_count   int         NOT NULL DEFAULT 0,
    deleted       boolean     NOT NULL DEFAULT false,
    text_stale    boolean     NOT NULL DEFAULT false,
    committed_at  timestamptz,
    applied_at    timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (generation_id, row_key)
);
