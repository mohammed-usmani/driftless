package dev.driftless.server.generation;

import dev.driftless.server.embed.EmbeddingSpec;
import java.util.List;

/** One build of a pipeline's index: a model, a template, chunking settings and its own chunk table. */
public record Generation(long id, String pipeline, int number, EmbeddingSpec embedding, String template,
                         int maxChars, int overlapChars, List<String> metadataColumns, String configHash,
                         GenerationStatus status, String chunkTable) {
}
