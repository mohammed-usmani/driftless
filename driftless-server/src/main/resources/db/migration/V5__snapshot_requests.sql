CREATE TABLE snapshot_request (
    id            bigserial   PRIMARY KEY,
    pipeline      text        NOT NULL,
    kind          text        NOT NULL,
    keys          text[]      NOT NULL DEFAULT '{}',
    generation_id bigint,
    reason        text        NOT NULL,
    status        text        NOT NULL,
    signal_id     text,
    created_at    timestamptz NOT NULL DEFAULT now(),
    started_at    timestamptz,
    finished_at   timestamptz
);
CREATE INDEX snapshot_request_pipeline ON snapshot_request (pipeline, status, id);

CREATE TABLE shedlock (
    name       varchar(64)  PRIMARY KEY,
    lock_until timestamptz  NOT NULL,
    locked_at  timestamptz  NOT NULL,
    locked_by  varchar(255) NOT NULL
);
