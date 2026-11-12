CREATE TABLE failed_change (
    id            bigserial   PRIMARY KEY,
    pipeline      text        NOT NULL,
    row_key       text,
    version_major bigint,
    version_minor bigint,
    reason        text        NOT NULL,
    payload       text,
    status        text        NOT NULL DEFAULT 'OPEN',
    created_at    timestamptz NOT NULL DEFAULT now(),
    resolved_at   timestamptz
);
CREATE INDEX failed_change_open ON failed_change (pipeline, status, id);

CREATE TABLE recent_change (
    id              bigserial   PRIMARY KEY,
    pipeline        text        NOT NULL,
    generation_id   bigint      NOT NULL,
    row_key         text,
    outcome         text        NOT NULL,
    chunks_embedded int         NOT NULL,
    cache_hits      int         NOT NULL,
    freshness_ms    bigint      NOT NULL,
    snapshot        boolean     NOT NULL,
    applied_at      timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX recent_change_pipeline ON recent_change (pipeline, id DESC);
