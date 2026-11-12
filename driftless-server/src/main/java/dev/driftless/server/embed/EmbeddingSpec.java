package dev.driftless.server.embed;

import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.Pattern;

/** Which model embeds a pipeline's chunks. 2000 is pgvector's HNSW limit for the {@code vector} type. */
public record EmbeddingSpec(
        @NotBlank @Pattern(regexp = "ollama|openai|fake") String provider,
        @NotBlank String model,
        @Min(1) @Max(2000) int dimensions) {
}
