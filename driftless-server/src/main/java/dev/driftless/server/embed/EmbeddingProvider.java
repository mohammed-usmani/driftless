package dev.driftless.server.embed;

import java.util.List;

/**
 * Turns texts into vectors, one per text, in order. Throws {@link ProviderUnavailableException} when a
 * retry may help (5xx, 429, timeouts, connection errors) and {@link IllegalArgumentException} when it won't.
 */
public interface EmbeddingProvider {
    List<float[]> embed(EmbeddingSpec spec, List<String> texts);
}
