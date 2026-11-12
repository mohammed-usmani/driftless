# 1. Row versions come from Debezium's `source.sequence`

Status: accepted

## Context

Every change Driftless applies carries a version, and a change is applied only if its version is newer than
the one stored for that row. This is what makes redelivery, consumer rebalances, replays, incremental
snapshots and index migrations safe. The version must therefore order every record of a row the way the
source committed them, including the `r` records an incremental snapshot emits.

The obvious candidate, the change's own `source.lsn`, does not work:

- incremental-snapshot reads have no `lsn` at all (it is `null`);
- change LSNs are written when a statement runs, not when its transaction commits, so across
  transactions they are not in commit order. pgoutput streams transactions in commit order, so the
  stream itself is the ordering we need to capture.

## Decision

`Version = (major, minor)` where, from the record's `source.sequence` (a JSON array encoded as a string,
`["lastCommitLsn","lsn"]`):

- `major` = `lastCommitLsn`, `minor` = `lsn`, with `null` treated as 0;
- for incremental-snapshot reads Debezium sets both to the stream position at which the read was
  emitted (the window's high watermark), which places the read after every change it reflects and
  before every change that follows it.

The reconciler's synthetic delete for a leaked row does not read a WAL position. It takes the version
Driftless last applied for that row and uses `justAfter()` (`minor + 1`): it wins only if nothing newer was
applied, and any real later change has a larger `major` and wins over it. An earlier draft used
`pg_current_wal_lsn()` for this; that is unsafe, because a transaction can write its change before that
read and commit after it, and would then lose to the synthetic delete.

## Evidence

`VersionOrderingIT` runs four writers doing multi-statement transactions (inserts, title-only updates,
TOASTed body updates, deletes) on 50 hot keys for 20 seconds, with two full incremental snapshots and a
connector restart in the middle. It asserts that, per key and in stream order, streamed versions strictly
increase and snapshot reads never go backwards, and that the max-version record of every key equals the
source's final row (values compared by hash, which also proves no TOAST placeholders survive).

A naive `lsn` version fails it immediately (snapshot reads sort first). The sequence-based version passed
three consecutive runs.

## Consequences

- This depends on connector behaviour, so the Debezium version is pinned (**3.7.1.Final**) and
  `VersionOrderingIT` runs in CI; an upgrade that changes `sequence` fails it.
- The first transaction a brand-new slot streams has a `null` last commit (major 0). That is harmless:
  nothing was applied for its rows before it.
