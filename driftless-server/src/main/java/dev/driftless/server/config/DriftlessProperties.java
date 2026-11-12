package dev.driftless.server.config;

import jakarta.validation.Valid;
import java.math.BigDecimal;
import java.util.List;
import java.util.Map;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.bind.DefaultValue;
import org.springframework.validation.annotation.Validated;

@Validated
@ConfigurationProperties("driftless")
public record DriftlessProperties(
        @Valid @DefaultValue Source source,
        List<@Valid PipelineConfig> pipelines,
        @DefaultValue("http://localhost:8083") String kafkaConnectUrl,
        String adminToken,
        @Valid @DefaultValue Reconciler reconciler,
        @Valid @DefaultValue Embedding embedding) {

    public DriftlessProperties {
        pipelines = pipelines == null ? List.of() : List.copyOf(pipelines);
    }

    /**
     * The source database. {@code url} is how Driftless reads it; {@code connectHost}/{@code connectPort}
     * are how Debezium reaches it, which differ when they sit on different networks (tests).
     */
    public record Source(String url, String user, String password, String connectHost, Integer connectPort) { }

    public record Reconciler(
            @DefaultValue("200") int sampleSize,
            @DefaultValue("30") int graceSeconds,
            @DefaultValue("false") boolean autoRepair) { }

    public record Embedding(
            @DefaultValue("http://localhost:11434") String ollamaUrl,
            @DefaultValue("https://api.openai.com") String openaiUrl,
            String openaiKey,
            @DefaultValue("64") int batchSize,
            @DefaultValue("0") int fakeLatencyMs,
            Map<String, BigDecimal> pricePer1kTokens) {

        public Embedding {
            pricePer1kTokens = pricePer1kTokens == null ? Map.of() : Map.copyOf(pricePer1kTokens);
        }
    }

    public PipelineConfig pipeline(String name) {
        return pipelines.stream().filter(p -> p.name().equals(name)).findFirst().orElse(null);
    }
}
