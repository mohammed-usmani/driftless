CREATE TABLE drift_report (
    id             bigserial   PRIMARY KEY,
    pipeline       text        NOT NULL,
    generation_id  bigint      NOT NULL,
    started_at     timestamptz NOT NULL,
    finished_at    timestamptz NOT NULL,
    sampled_source int         NOT NULL,
    sampled_index  int         NOT NULL,
    missing        int         NOT NULL,
    mismatched     int         NOT NULL,
    leaked         int         NOT NULL,
    repaired       int         NOT NULL DEFAULT 0
);
CREATE INDEX drift_report_pipeline ON drift_report (pipeline, id DESC);

CREATE TABLE drift_item (
    report_id bigint  NOT NULL REFERENCES drift_report (id) ON DELETE CASCADE,
    row_key   text    NOT NULL,
    kind      text    NOT NULL,
    detail    text,
    repaired  boolean NOT NULL DEFAULT false,
    PRIMARY KEY (report_id, row_key)
);
