-- untyped vector: one cache serves models of every dimension
CREATE TABLE embedding_cache (
    model        text        NOT NULL,
    chunk_hash   text        NOT NULL,
    embedding    vector      NOT NULL,
    last_used_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (model, chunk_hash)
);
CREATE INDEX embedding_cache_last_used ON embedding_cache (last_used_at);
