package dev.driftless.server.generation;

import dev.driftless.server.config.DriftlessProperties;
import dev.driftless.server.config.PipelineConfig;
import dev.driftless.server.source.ConnectorManager;
import dev.driftless.server.source.SourcePreflight;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Component;

/**
 * Brings each configured pipeline up at startup: preflight, first generation, connector. A pipeline that
 * fails preflight is BLOCKED with the reason and left alone; the others start normally.
 */
@Component
public class PipelineBootstrap {

    private static final Logger log = LoggerFactory.getLogger(PipelineBootstrap.class);

    private final DriftlessProperties props;
    private final SourcePreflight preflight;
    private final GenerationRepository generations;
    private final ChunkStore chunks;
    private final ConnectorManager connectors;
    private final ApplicationEventPublisher events;

    public PipelineBootstrap(DriftlessProperties props, SourcePreflight preflight, GenerationRepository generations,
                             ChunkStore chunks, ConnectorManager connectors, ApplicationEventPublisher events) {
        this.props = props;
        this.preflight = preflight;
        this.generations = generations;
        this.chunks = chunks;
        this.connectors = connectors;
        this.events = events;
    }

    @EventListener(ApplicationReadyEvent.class)
    public void start() {
        props.pipelines().forEach(this::run);
    }

    public void run(PipelineConfig p) {
        List<String> problems = preflight.check(p);
        if (!problems.isEmpty()) {
            String reason = String.join("; ", problems);
            log.warn("pipeline {} is blocked: {}", p.name(), reason);
            generations.upsertPipeline(p, PipelineStatus.BLOCKED, reason);
            return;
        }
        generations.upsertPipeline(p, PipelineStatus.ACTIVE, null);
        Generation created = generations.createFirstIfAbsent(p).orElse(null);
        generations.targets(p.name()).forEach(chunks::createTable);
        connectors.ensure(p);
        if (created != null) {
            events.publishEvent(new GenerationCreated(created, false));
        }
        log.info("pipeline {} is active", p.name());
    }
}
