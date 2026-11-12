package dev.driftless.server.reconcile;

import java.time.Instant;

public record DriftReport(long id, String pipeline, long generationId, Instant startedAt, Instant finishedAt,
                          int sampledSource, int sampledIndex, int missing, int mismatched, int leaked, int repaired) {

    public enum Kind { MISSING, MISMATCH, LEAK }

    public record Item(String key, Kind kind, String detail, boolean repaired) { }
}
