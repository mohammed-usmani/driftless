package dev.driftless.server.generation;

import dev.driftless.server.api.ConflictException;
import dev.driftless.server.api.NotFoundException;
import dev.driftless.server.config.DriftlessProperties;
import dev.driftless.server.config.PipelineConfig;
import dev.driftless.server.source.ConnectorManager;
import dev.driftless.server.source.KafkaOffsets;
import dev.driftless.server.source.SnapshotFinished;
import dev.driftless.server.source.SnapshotQueue;
import dev.driftless.server.source.SourceDatabase;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.context.event.EventListener;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import tools.jackson.core.type.TypeReference;
import tools.jackson.databind.json.JsonMapper;

/**
 * Re-embeds a pipeline into a new generation while the old one keeps serving. The new generation gets every
 * live change from the moment it exists plus a full snapshot; it is READY once the consumer has applied that
 * snapshot, and cutover then swaps which generation is LIVE in one transaction.
 */
@Service
public class MigrationService {

    private static final Logger log = LoggerFactory.getLogger(MigrationService.class);
    private static final JsonMapper JSON = JsonMapper.builder().build();

    public record Progress(long rows, long sourceRows, GenerationStatus status) { }

    private final DriftlessProperties props;
    private final GenerationRepository generations;
    private final RowStateRepository rows;
    private final ChunkStore chunks;
    private final ConnectorManager connectors;
    private final SnapshotQueue snapshots;
    private final SourceDatabase.Source source;
    private final JdbcTemplate jdbc;
    private final ApplicationEventPublisher events;
    private final KafkaOffsets offsets;

    public MigrationService(DriftlessProperties props, GenerationRepository generations, RowStateRepository rows,
                            ChunkStore chunks, ConnectorManager connectors, SnapshotQueue snapshots,
                            SourceDatabase.Source source, JdbcTemplate jdbc, ApplicationEventPublisher events,
                            KafkaOffsets offsets) {
        this.props = props;
        this.generations = generations;
        this.rows = rows;
        this.chunks = chunks;
        this.connectors = connectors;
        this.snapshots = snapshots;
        this.source = source;
        this.jdbc = jdbc;
        this.events = events;
        this.offsets = offsets;
    }

    @Transactional
    public Generation start(String pipeline) {
        PipelineConfig p = config(pipeline);
        lock(pipeline);
        Generation live = generations.live(pipeline).orElseThrow(() -> new ConflictException("pipeline " + pipeline + " has no live generation"));
        if (live.configHash().equals(p.configHash())) {
            throw new ConflictException("config unchanged: the live generation already matches it");
        }
        if (generations.targets(pipeline).size() > 1) {
            throw new ConflictException("a migration is already active for " + pipeline);
        }
        Generation g = generations.create(p, GenerationStatus.BUILDING);
        chunks.createTable(g);
        events.publishEvent(new GenerationCreated(g, true)); // queues the MIGRATION snapshot in this transaction
        log.info("migration of {} to generation {} started", pipeline, g.number());
        return g;
    }

    @Transactional
    public void cutover(String pipeline) {
        lock(pipeline);
        Generation next = only(pipeline, GenerationStatus.READY, "no READY generation to cut over to");
        Generation live = generations.live(pipeline).orElseThrow();
        swap(pipeline, live, GenerationStatus.DRAINING, next);
        log.info("{} cut over to generation {}", pipeline, next.number());
    }

    @Transactional
    public void rollback(String pipeline) {
        lock(pipeline);
        Generation old = only(pipeline, GenerationStatus.DRAINING, "nothing to roll back to");
        Generation live = generations.live(pipeline).orElseThrow();
        swap(pipeline, live, GenerationStatus.READY, old);
        log.info("{} rolled back to generation {}", pipeline, old.number());
    }

    @Transactional
    public void finalizeMigration(String pipeline) {
        lock(pipeline);
        Generation old = only(pipeline, GenerationStatus.DRAINING, "no DRAINING generation to finalize");
        generations.setStatus(old.id(), GenerationStatus.DROPPED);
        jdbc.update("DELETE FROM row_state WHERE generation_id = ?", old.id());
        chunks.dropTable(old);
        log.info("{} generation {} dropped", pipeline, old.number());
    }

    public Progress progress(String pipeline) {
        PipelineConfig p = config(pipeline);
        Generation g = generations.targets(pipeline).stream()
                .filter(x -> x.status() == GenerationStatus.BUILDING || x.status() == GenerationStatus.READY)
                .findFirst().orElseThrow(() -> new NotFoundException("no migration in progress for " + pipeline));
        long sourceRows = source.jdbc().queryForObject("SELECT count(*) FROM " + p.table(), Long.class);
        return new Progress(rows.countLive(g.id()), sourceRows, g.status());
    }

    /** The snapshot was sent to Kafka; remember how far the change topic reached so READY waits for the consumer. */
    @EventListener
    public void onSnapshotFinished(SnapshotFinished f) {
        if (f.reason() != SnapshotQueue.Reason.MIGRATION || f.generationId() == null) {
            return;
        }
        if (!f.completed()) {
            log.warn("migration snapshot for {} was aborted; requesting it again", f.pipeline());
            snapshots.request(f.pipeline(), SnapshotQueue.Kind.FULL, List.of(), f.generationId(), SnapshotQueue.Reason.MIGRATION);
            return;
        }
        Map<Integer, Long> ends = offsets.end(connectors.topic(config(f.pipeline())));
        jdbc.update("UPDATE generation SET ready_offsets = ?::jsonb WHERE id = ? AND status = 'BUILDING'",
                JSON.writeValueAsString(ends), f.generationId());
    }

    @Scheduled(fixedDelayString = "${driftless.migration-check-ms:1000}")
    public void promoteCaughtUp() {
        for (Map<String, Object> row : jdbc.queryForList(
                "SELECT id, pipeline, ready_offsets::text AS offsets FROM generation WHERE status = 'BUILDING' AND ready_offsets IS NOT NULL")) {
            String pipeline = (String) row.get("pipeline");
            PipelineConfig p = props.pipeline(pipeline);
            if (p == null) {
                continue;
            }
            Map<Integer, Long> needed = JSON.readValue((String) row.get("offsets"), new TypeReference<Map<Integer, Long>>() { });
            Map<Integer, Long> committed = offsets.committed(connectors.topic(p));
            boolean caughtUp = needed.entrySet().stream().allMatch(e -> committed.getOrDefault(e.getKey(), 0L) >= e.getValue());
            if (caughtUp && jdbc.update("UPDATE generation SET status = 'READY', ready_at = now() WHERE id = ? AND status = 'BUILDING'",
                    row.get("id")) == 1) {
                log.info("{} generation {} is READY", pipeline, row.get("id"));
            }
        }
    }

    private void swap(String pipeline, Generation from, GenerationStatus fromStatus, Generation to) {
        generations.setStatus(from.id(), fromStatus); // first: only one LIVE per pipeline is allowed
        generations.setStatus(to.id(), GenerationStatus.LIVE);
        generations.setLive(pipeline, to.id());
    }

    private Generation only(String pipeline, GenerationStatus status, String otherwise) {
        Optional<Generation> g = generations.targets(pipeline).stream().filter(x -> x.status() == status).findFirst();
        return g.orElseThrow(() -> new ConflictException(otherwise));
    }

    private PipelineConfig config(String pipeline) {
        PipelineConfig p = props.pipeline(pipeline);
        if (p == null) {
            throw new NotFoundException("no pipeline " + pipeline);
        }
        return p;
    }

    private void lock(String pipeline) {
        jdbc.queryForList("SELECT name FROM pipeline WHERE name = ? FOR UPDATE", pipeline);
    }
}
