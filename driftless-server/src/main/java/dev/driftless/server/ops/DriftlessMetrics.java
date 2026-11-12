package dev.driftless.server.ops;

import dev.driftless.server.api.NotFoundException;
import dev.driftless.server.apply.Applied;
import dev.driftless.server.config.DriftlessProperties;
import dev.driftless.server.config.PipelineConfig;
import dev.driftless.server.embed.EmbeddingService;
import dev.driftless.server.generation.MigrationService;
import dev.driftless.server.source.ConnectorManager;
import dev.driftless.server.source.KafkaOffsets;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import java.math.BigDecimal;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

/** Driftless's Prometheus metrics, all tagged by pipeline. */
@Component
public class DriftlessMetrics {

    private static final Duration[] FRESHNESS_SLOS = {Duration.ofMillis(250), Duration.ofMillis(500), Duration.ofSeconds(1),
            Duration.ofSeconds(2), Duration.ofSeconds(5), Duration.ofSeconds(10), Duration.ofSeconds(30)};

    private final MeterRegistry registry;
    private final DriftlessProperties props;
    private final KafkaOffsets offsets;
    private final ConnectorManager connectors;
    private final ObjectProvider<MigrationService> migrations; // migrations → … → metrics would be a cycle
    private final Map<String, Double> lag = new ConcurrentHashMap<>();
    private final Map<String, Double> progress = new ConcurrentHashMap<>();

    public DriftlessMetrics(MeterRegistry registry, DriftlessProperties props, EmbeddingService embeddings, KafkaOffsets offsets,
                            ConnectorManager connectors, ObjectProvider<MigrationService> migrations) {
        this.registry = registry;
        this.props = props;
        this.offsets = offsets;
        this.connectors = connectors;
        this.migrations = migrations;
        for (PipelineConfig p : props.pipelines()) {
            String name = p.name();
            Gauge.builder("driftless.provider.circuit.open", embeddings, e -> e.circuitOpen(p.embedding().provider()) ? 1 : 0)
                    .tag("pipeline", name).register(registry);
            Gauge.builder("driftless.consumer.lag.records", lag, m -> m.getOrDefault(name, 0.0)).tag("pipeline", name).register(registry);
            Gauge.builder("driftless.migration.progress.ratio", progress, m -> m.getOrDefault(name, 0.0)).tag("pipeline", name).register(registry);
        }
    }

    public void applied(List<Applied> applied) {
        for (Applied a : applied) {
            Counter.builder("driftless.changes").tag("pipeline", a.pipeline()).tag("outcome", a.outcome().name()).register(registry).increment();
            Timer.builder("driftless.freshness").tag("pipeline", a.pipeline()).serviceLevelObjectives(FRESHNESS_SLOS)
                    .register(registry).record(Duration.ofMillis(a.freshnessMs()));
        }
    }

    /** Cost is estimated at chars / 4 tokens, priced per 1k tokens by model from config. */
    public void embedded(String pipeline, String model, int hits, int misses, int providerCalls, long missedChars) {
        Counter.builder("driftless.embed.chunks").tag("pipeline", pipeline).tag("result", "hit").register(registry).increment(hits);
        Counter.builder("driftless.embed.chunks").tag("pipeline", pipeline).tag("result", "miss").register(registry).increment(misses);
        Counter.builder("driftless.embed.provider.calls").tag("pipeline", pipeline).register(registry).increment(providerCalls);
        BigDecimal price = props.embedding().pricePer1kTokens().getOrDefault(model, BigDecimal.ZERO);
        Counter.builder("driftless.embed.estimated.cost.usd").tag("pipeline", pipeline).register(registry)
                .increment(price.doubleValue() * missedChars / 4.0 / 1000.0);
    }

    public void drift(String pipeline, String kind) {
        Counter.builder("driftless.drift.items").tag("pipeline", pipeline).tag("kind", kind).register(registry).increment();
    }

    @Scheduled(fixedDelayString = "${driftless.metrics-refresh:PT15S}", initialDelay = 5_000)
    void refresh() {
        for (PipelineConfig p : props.pipelines()) {
            try {
                lag.put(p.name(), (double) offsets.lag(connectors.topic(p)));
            } catch (RuntimeException e) {
                lag.remove(p.name()); // topic not created yet
            }
            try {
                MigrationService.Progress m = migrations.getObject().progress(p.name());
                progress.put(p.name(), m.sourceRows() == 0 ? 1.0 : Math.min(1.0, (double) m.rows() / m.sourceRows()));
            } catch (NotFoundException e) {
                progress.put(p.name(), 0.0);
            } catch (RuntimeException e) {
                // keep the last value
            }
        }
    }
}
