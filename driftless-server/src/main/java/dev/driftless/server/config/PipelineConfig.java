package dev.driftless.server.config;

import dev.driftless.server.embed.EmbeddingSpec;
import jakarta.validation.Valid;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Pattern;
import java.util.List;
import org.springframework.boot.context.properties.bind.DefaultValue;

/**
 * One synced table. The name ends up in table, slot and topic names, so it is restricted to a
 * safe identifier; nothing else from config reaches dynamic SQL unquoted.
 */
public record PipelineConfig(
        @NotBlank @Pattern(regexp = "^[a-z][a-z0-9_]{0,40}$") String name,
        @NotBlank @Pattern(regexp = "^([a-z_][a-z0-9_]*\\.)?[a-z_][a-z0-9_]*$") String table,
        @NotBlank @Pattern(regexp = "^[a-z_][a-z0-9_]*$") String key,
        @NotBlank String text,
        List<@Pattern(regexp = "^[a-z_][a-z0-9_]*$") String> metadata,
        @Valid @NotNull EmbeddingSpec embedding,
        @Valid @DefaultValue Chunking chunking) {

    public PipelineConfig {
        metadata = metadata == null ? List.of() : List.copyOf(metadata);
    }

    public record Chunking(@DefaultValue("2000") @Min(100) int maxChars, @DefaultValue("200") @Min(0) int overlapChars) { }

    /** Identifies everything that shapes a generation's index; a different hash needs a migration. */
    public String configHash() {
        return dev.driftless.server.change.Hashes.sha256Hex(String.join("\u0001", embedding.provider(), embedding.model(),
                Integer.toString(embedding.dimensions()), text, Integer.toString(chunking.maxChars()),
                Integer.toString(chunking.overlapChars()), String.join(",", metadata)));
    }

    public String schema() {
        int dot = table.indexOf('.');
        return dot < 0 ? "public" : table.substring(0, dot);
    }

    public String tableName() {
        int dot = table.indexOf('.');
        return dot < 0 ? table : table.substring(dot + 1);
    }
}
