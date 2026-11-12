package dev.driftless.server.apply;

/** One change applied to one generation, as recorded in recent_change. */
public record Applied(String pipeline, long generationId, String key, Outcome outcome, int embedded, int cacheHits,
                      long freshnessMs, boolean snapshot) {
}
