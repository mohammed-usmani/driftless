package dev.driftless.server.source;

import dev.driftless.server.config.DriftlessProperties;
import dev.driftless.server.config.PipelineConfig;
import dev.driftless.server.generation.GenerationCreated;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import net.javacrumbs.shedlock.spring.annotation.SchedulerLock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.context.event.EventListener;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

/**
 * Incremental snapshots, one at a time per pipeline. Debezium queues signals itself, but running them one at
 * a time is what lets a completion notification be matched to the request that caused it.
 */
@Component
public class SnapshotQueue {

    private static final Logger log = LoggerFactory.getLogger(SnapshotQueue.class);
    static final int MAX_KEYS_PER_SIGNAL = 200;

    public enum Kind { FULL, KEYS }

    public enum Reason { BACKFILL, MIGRATION, RETRY, REPAIR }

    private final JdbcTemplate jdbc;
    private final DriftlessProperties props;
    private final SignalSender signals;
    private final SourceDatabase.Source source;
    private final ApplicationEventPublisher events;

    public SnapshotQueue(JdbcTemplate jdbc, DriftlessProperties props, SignalSender signals,
                         SourceDatabase.Source source, ApplicationEventPublisher events) {
        this.jdbc = jdbc;
        this.props = props;
        this.signals = signals;
        this.source = source;
        this.events = events;
    }

    /** Queues a snapshot; a key list is split into requests of at most 200 keys. Returns the request ids. */
    public List<Long> request(String pipeline, Kind kind, List<String> keys, Long generationId, Reason reason) {
        List<List<String>> parts = new ArrayList<>();
        if (kind == Kind.FULL) {
            parts.add(List.of());
        } else {
            for (int i = 0; i < keys.size(); i += MAX_KEYS_PER_SIGNAL) {
                parts.add(keys.subList(i, Math.min(keys.size(), i + MAX_KEYS_PER_SIGNAL)));
            }
        }
        List<Long> ids = new ArrayList<>();
        for (List<String> part : parts) {
            ids.add(jdbc.queryForObject("""
                    INSERT INTO snapshot_request (pipeline, kind, keys, generation_id, reason, status)
                    VALUES (?, ?, ?, ?, ?, 'QUEUED') RETURNING id""", Long.class,
                    pipeline, kind.name(), part.toArray(String[]::new), generationId, reason.name()));
        }
        return ids;
    }

    @EventListener
    public void onGenerationCreated(GenerationCreated e) {
        request(e.generation().pipeline(), Kind.FULL, List.of(), e.generation().id(), e.migration() ? Reason.MIGRATION : Reason.BACKFILL);
    }

    /**
     * Starts the oldest queued request of each idle pipeline. A signal written before Debezium's slot is
     * streaming would never be seen, so a pipeline whose slot isn't active waits.
     */
    @Scheduled(fixedDelayString = "${driftless.snapshot-dispatch-ms:500}")
    @SchedulerLock(name = "snapshot-dispatch", lockAtMostFor = "PT1M")
    public void dispatch() {
        for (PipelineConfig p : props.pipelines()) {
            try {
                dispatch(p);
            } catch (RuntimeException e) {
                log.warn("could not start a snapshot for {}: {}", p.name(), e.getMessage());
            }
        }
    }

    /** Only ever runs on one instance at a time (ShedLock), so no row locks are needed here. */
    void dispatch(PipelineConfig p) {
        if (Boolean.TRUE.equals(jdbc.queryForObject(
                "SELECT exists(SELECT 1 FROM snapshot_request WHERE pipeline = ? AND status = 'RUNNING')", Boolean.class, p.name()))) {
            return;
        }
        List<Map<String, Object>> next = jdbc.queryForList("""
                SELECT id, keys FROM snapshot_request WHERE pipeline = ? AND status = 'QUEUED'
                ORDER BY id LIMIT 1""", p.name());
        if (next.isEmpty() || !slotActive(p.name())) {
            return;
        }
        long id = ((Number) next.getFirst().get("id")).longValue();
        List<String> keys = keys(next.getFirst().get("keys"));
        String signal = signals.executeSnapshot(p, keys);
        jdbc.update("UPDATE snapshot_request SET status = 'RUNNING', signal_id = ?, started_at = now() WHERE id = ?", signal, id);
        log.info("started snapshot request {} for {} ({} keys)", id, p.name(), keys.isEmpty() ? "all" : keys.size());
    }

    /** Called when Debezium reports the pipeline's running snapshot finished. */
    public Optional<SnapshotFinished> finish(String pipeline, boolean completed) {
        List<Map<String, Object>> running = jdbc.queryForList("""
                UPDATE snapshot_request SET status = ?, finished_at = now()
                WHERE pipeline = ? AND status = 'RUNNING' RETURNING id, generation_id, reason""",
                completed ? "COMPLETED" : "ABORTED", pipeline);
        if (running.isEmpty()) {
            return Optional.empty();
        }
        Map<String, Object> r = running.getFirst();
        SnapshotFinished done = new SnapshotFinished(((Number) r.get("id")).longValue(), pipeline,
                r.get("generation_id") == null ? null : ((Number) r.get("generation_id")).longValue(),
                Reason.valueOf((String) r.get("reason")), completed);
        events.publishEvent(done);
        return Optional.of(done);
    }

    private boolean slotActive(String pipeline) {
        return Boolean.TRUE.equals(source.jdbc().queryForObject(
                "SELECT exists(SELECT 1 FROM pg_replication_slots WHERE slot_name = ? AND active)", Boolean.class,
                "driftless_" + pipeline));
    }

    private static List<String> keys(Object array) {
        try {
            return array == null ? List.of() : List.of((String[]) ((java.sql.Array) array).getArray());
        } catch (java.sql.SQLException e) {
            throw new IllegalStateException(e);
        }
    }
}
