# 2. One change stream: Debezium on Kafka for live changes, backfill, migration and repair

Status: accepted

## Context

Driftless has four reasons to (re)process a source row: a live change, the initial backfill of a new
pipeline, the full re-read a model migration needs, and repair of a row the reconciler found drifted.
The usual design has a CDC path for the first and a bulk "SELECT and embed" path for the rest. The bulk
path is where index-sync bugs come from: it reads a row, a live update to that row commits, and the bulk
write lands after the update and puts the old text back.

The options considered for the live path:

- **Polling `updated_at`** needs a column the application must maintain, and never sees deletes.
- **Triggers** into an outbox table change the source schema and add write latency to the application.
- **The embedded Debezium engine** inside Driftless keeps offsets in-process. Two instances can't share
  the work, and there's no way to replay a range after a bug.
- **Debezium on Kafka Connect** keeps offsets and the replication slot outside Driftless, and writes a
  replayable, key-partitioned log.

## Decision

Debezium (Kafka Connect, pgoutput) streams each pipeline's table to one Kafka topic, keyed by primary key,
with 6 partitions. Backfill, migration and repair are **incremental snapshots** requested through
Debezium's signal table. Their read events arrive on the same topic, merged with the live stream by
Debezium's watermarking, and go through the same apply code as live changes. The one exception is leak
repair ([ADR 5](0005-leak-repair-by-synthetic-delete.md)), and even that is published onto the topic.

Each pipeline gets its own connector, replication slot and publication, so one pipeline's long snapshot
never queues behind another's. Driftless upserts connector configs through the Connect REST API at startup.

Kafka rather than RabbitMQ (which HookRelay uses): here ordering per key and replay matter more than
per-message routing and delay queues. A key always lands on the same partition, and a consumer group
splits partitions across instances.

## Consequences

- There's no bulk code path to keep correct. The versioned apply ([ADR 1](0001-row-versions.md)) is the only
  thing standing between the stream and the index, and `ConvergenceIT` tests it under random writes,
  redelivery and snapshots together.
- Driftless can issue one incremental snapshot per connector at a time, so it keeps its own queue of
  snapshot requests (`snapshot_request`). A retry or repair during a migration backfill waits for it.
- The source needs `wal_level=logical`, `REPLICA IDENTITY FULL` and the signal table in the publication.
  A startup preflight checks them and blocks only the affected pipeline.
- Running Kafka and Connect is real operational weight for a small deployment. It's the price of replay and
  of sharing work across instances.
