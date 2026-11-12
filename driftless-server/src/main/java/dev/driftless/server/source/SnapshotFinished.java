package dev.driftless.server.source;

/** Published when Debezium reports an incremental snapshot finished. */
public record SnapshotFinished(long requestId, String pipeline, Long generationId, SnapshotQueue.Reason reason, boolean completed) {
}
