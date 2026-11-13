# 3. One chunk table per index generation

Status: accepted

## Context

A generation is everything that decides what the index contains: model, dimensions, text template,
chunking and metadata columns. A migration builds generation n+1 while n keeps serving search, then swaps.
Two generations of one pipeline often have different vector dimensions (768 for `nomic-embed-text`, 1024
for `mxbai-embed-large`).

A single chunk table with a `generation` column would need an untyped `vector` column. pgvector's HNSW
index needs a fixed dimension, so that table could only be indexed with one partial expression index per
generation, each cast to its own dimension. Dropping an old generation would also mean a large `DELETE`
plus vacuum on the table search is reading.

## Decision

Each generation gets its own table, `chunks_<pipeline>_g<n>`, with `embedding vector(d)`, an HNSW cosine
index and a GIN index on the metadata. Generation state (`BUILDING → READY → LIVE → DRAINING → DROPPED`)
lives in the `generation` table, and a unique index allows only one `LIVE` generation per pipeline.
`row_state` is keyed by `(generation, row key)`, so each generation tracks its own versions and tombstones.

Search reads only the `LIVE` generation's table. Cutover and rollback are one transaction that changes two
status rows. Finalize drops the table.

## Consequences

- A migration can change dimensions freely, and each table's HNSW index is ordinary.
- Dropping a generation is `DROP TABLE`, which is instant and doesn't bloat the live table.
- Table names are built from config, so pipeline names are restricted to `[a-z0-9_]` and validated at startup.
- While a migration runs, every change is applied to two tables. Embeddings are cached by (model, chunk
  hash), so a migration that keeps the model, and only changes chunking or metadata, mostly reuses them.
