package dev.driftless.server.reconcile;

import dev.driftless.server.change.RowKeys;
import dev.driftless.server.change.Version;
import dev.driftless.server.config.DriftlessProperties;
import dev.driftless.server.config.PipelineConfig;
import dev.driftless.server.source.ConnectorManager;
import dev.driftless.server.source.SourceReader;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;
import tools.jackson.databind.json.JsonMapper;

/**
 * Deletes rows that are indexed but gone from the source. A snapshot can't do it (it only reads rows that
 * exist), so a synthetic Debezium delete is put on the pipeline's own topic: same key bytes, same partition,
 * so it is ordered behind anything already in flight and goes through the normal apply path. Its version is
 * just after the last one applied, so any later real change for the key wins (ADR 0001).
 */
@Component
public class LeakRepair {

    private static final Logger log = LoggerFactory.getLogger(LeakRepair.class);
    private static final JsonMapper JSON = JsonMapper.builder().build();

    private final DriftlessProperties props;
    private final SourceReader reader;
    private final ConnectorManager connectors;
    private final KafkaTemplate<byte[], byte[]> kafka;
    private final JdbcTemplate jdbc;

    public LeakRepair(DriftlessProperties props, SourceReader reader, ConnectorManager connectors,
                      KafkaTemplate<byte[], byte[]> kafka, JdbcTemplate jdbc) {
        this.props = props;
        this.reader = reader;
        this.connectors = connectors;
        this.kafka = kafka;
        this.jdbc = jdbc;
    }

    /** Deletes the keys still absent from the source after the grace window. Returns how many deletes were published. */
    public int delete(PipelineConfig p, Collection<String> keys) {
        Set<String> absent = new LinkedHashSet<>(keys);
        absent.removeAll(reader.existing(p, absent));
        if (absent.isEmpty()) {
            return 0;
        }
        try {
            Thread.sleep(props.reconciler().graceSeconds() * 1000L);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return 0;
        }
        absent.removeAll(reader.existing(p, absent));
        int published = 0;
        for (String key : absent) {
            List<Version> last = jdbc.query("""
                    SELECT r.version_major, r.version_minor FROM row_state r JOIN generation g ON g.id = r.generation_id
                    WHERE g.pipeline = ? AND g.status <> 'DROPPED' AND r.row_key = ?
                    ORDER BY 1 DESC, 2 DESC LIMIT 1""", (rs, i) -> new Version(rs.getLong(1), rs.getLong(2)), p.name(), key);
            if (!last.isEmpty()) {
                publish(p, key, last.getFirst().justAfter());
                published++;
            }
        }
        log.info("published {} synthetic deletes for leaked rows of {}", published, p.name());
        return published;
    }

    /** Publishes a synthetic delete; returns the partition it landed on. */
    int publish(PipelineConfig p, String key, Version version) {
        Object typedKey = reader.integerKey(p) ? Long.parseLong(key) : key;
        long now = Instant.now().toEpochMilli();
        Map<String, Object> source = new LinkedHashMap<>();
        source.put("schema", p.schema());
        source.put("table", p.tableName());
        source.put("ts_ms", now);
        source.put("sequence", JSON.writeValueAsString(List.of(Long.toString(version.major()), Long.toString(version.minor()))));
        Map<String, Object> value = new LinkedHashMap<>();
        value.put("before", Map.of(p.key(), typedKey));
        value.put("after", null);
        value.put("op", "d");
        value.put("ts_ms", now);
        value.put("source", source);
        value.put("driftless", Map.of("synthetic", true));
        try {
            return kafka.send(connectors.topic(p), RowKeys.debeziumKeyJson(p.key(), typedKey).getBytes(StandardCharsets.UTF_8),
                    JSON.writeValueAsBytes(value)).get().getRecordMetadata().partition();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException(e);
        } catch (ExecutionException e) {
            throw new IllegalStateException("could not publish a synthetic delete for " + p.name() + " " + key, e.getCause());
        }
    }
}
