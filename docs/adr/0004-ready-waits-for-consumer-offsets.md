# 4. A migration is READY only when the consumer has passed the snapshot's end

Status: accepted

## Context

A migration backfills the new generation with an incremental snapshot. Debezium reports "snapshot
COMPLETED" on its notification topic, and the first implementation marked the generation `READY` on that
notification.

That is a race. The notification topic is consumed separately from the change topic. When the
notification arrives, the last snapshot reads may still be sitting unconsumed in the change topic. An
operator who cut over at that moment would serve search from a generation that was still missing rows.

## Decision

When the COMPLETED notification for a migration snapshot arrives, Driftless records the change topic's
**end offsets** per partition at that moment in `generation.ready_offsets`. Every read event of the
snapshot is already in the topic by then, before those offsets. A scheduled job (every second, ShedLocked)
then compares the consumer group's **committed** offsets with them. The generation becomes `READY` only
when every partition's committed offset has reached its recorded end offset.

Committed offsets advance only after a batch is fully applied, so by then every snapshot read is in the
new generation's table.

## Consequences

- `READY` means "safe to cut over": every row the snapshot read is in the new table, plus every live change
  committed before the snapshot ended.
- `READY` takes a few seconds longer than the notification. Under heavy lag it waits for the backlog to drain,
  and that backlog is exactly what cutover would otherwise have served stale.
- Committed offsets are read from Kafka with the admin client, so whichever instance holds the job's lock
  can promote the generation.
- `MigrationServiceIT.readyWaitsForTheConsumerToApplyTheSnapshot` stops the change listener, lets the snapshot
  complete, asserts the generation is still `BUILDING`, then restarts the listener and asserts that every row
  is present when `READY` arrives.
