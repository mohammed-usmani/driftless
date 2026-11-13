# 5. Leaks are repaired by a synthetic delete published onto the change topic

Status: accepted

## Context

A **leak** is a row that no longer exists in the source but is still in the index. It's the
privacy-relevant kind of drift: a deleted document, maybe deleted for a GDPR request, is still searchable.
Drifted rows that do exist are repaired by an incremental snapshot of their keys
([ADR 2](0002-one-change-stream.md)). That can't fix a leak, because a snapshot only reads rows that exist.

Deleting the chunks directly from the index would be a second writer that bypasses ordering. Suppose the row is
re-inserted in the source while the repair runs. The direct delete could land after the re-insert was
applied, and remove a row that now exists.

## Decision

The reconciler reports a leak only after it has checked twice that the row is gone (see grace below). The
repair then publishes a **synthetic Debezium delete** for that key onto the pipeline's own topic:

- with the same key bytes Debezium would write, so it lands on the same partition, ordered behind anything
  already in flight for that row;
- with version `justAfter()` the version last applied for the row ([ADR 1](0001-row-versions.md)). It wins
  only if nothing newer was applied, and a real later change, such as a re-insert, carries a larger WAL
  position and wins over it.

It then goes through the normal apply path: it becomes a delete with a tombstone, or it is dropped as stale.

**Grace re-check.** Every finding, not only leaks, is re-checked after `grace-seconds`. It stays in the
report only if the source row is unchanged (the same document and metadata hash, or still absent) and the
index still disagrees. A leak must also show the same `row_state` version both times. So a change that
reaches the index within the window is never reported, and a row re-inserted during the check is never
deleted. A change delayed longer than the window (a consumer backlog, a provider outage) can still be
reported; its repair is harmless, because a snapshot re-reads the current row and the synthetic delete loses
to anything newer, so the cost is a noisy report, not a wrong index.

## Consequences

- This is the one write to the topic that doesn't come from Debezium, and it's restricted to deletes of
  rows confirmed absent.
- `LeakRepairIT` re-inserts the key while the repair is waiting out its grace window, and asserts the
  re-insert survives. It also checks that the synthetic key bytes match what Debezium writes.
- The chaos endpoints (dev and demo profiles only) inject corruptions and leaks, so `scripts/demo.sh` can
  show detection and repair end to end.
