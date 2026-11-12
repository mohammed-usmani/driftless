package dev.driftless.server.embed;

import java.util.List;

/**
 * @param vectors one per requested chunk, in order
 * @param cacheHits distinct chunks served from the cache
 * @param embedded distinct chunks sent to the provider
 * @param providerCalls provider requests made (batches, not counting retries)
 */
public record EmbedResult(List<float[]> vectors, int cacheHits, int embedded, int providerCalls) {
}
