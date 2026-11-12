package dev.driftless.server.support;

import dev.driftless.server.config.PipelineConfig;
import dev.driftless.server.embed.EmbeddingSpec;
import java.util.List;

/** Pipeline configs for tests. Each test class uses its own name (= table name) so connectors and slots never collide. */
public final class Pipelines {

    private Pipelines() { }

    public static PipelineConfig articles(String name) {
        return articles(name, new EmbeddingSpec("fake", "fake-8", 8));
    }

    public static PipelineConfig articles(String name, EmbeddingSpec embedding) {
        return new PipelineConfig(name, "public." + name, "id", "{title}\n\n{body}",
                List.of("category", "tenant_id", "acl"), embedding, new PipelineConfig.Chunking(2000, 200));
    }
}
