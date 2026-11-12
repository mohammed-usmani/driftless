package dev.driftless.server.generation;

import dev.driftless.server.change.Version;
import java.time.Instant;

/** What a generation last applied for one source row. A deleted row keeps its state as a tombstone. */
public record RowState(long generationId, String key, Version version, String docHash, String metaHash,
                       int chunkCount, boolean deleted, boolean textStale, Instant committedAt) {
}
