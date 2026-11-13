package dev.driftless.server.api;

import dev.driftless.server.config.DriftlessProperties;
import dev.driftless.server.config.PipelineConfig;
import dev.driftless.server.generation.ChunkStore;
import dev.driftless.server.generation.Generation;
import dev.driftless.server.generation.GenerationRepository;
import dev.driftless.server.generation.MigrationService;
import dev.driftless.server.source.ConnectorManager;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

/** Read-only views of pipelines for the console. */
@RestController
class PipelineController {

    private final DriftlessProperties props;
    private final GenerationRepository generations;
    private final MigrationService migrations;
    private final ConnectorManager connectors;
    private final JdbcTemplate jdbc;

    PipelineController(DriftlessProperties props, GenerationRepository generations, MigrationService migrations,
                       ConnectorManager connectors, JdbcTemplate jdbc) {
        this.connectors = connectors;
        this.props = props;
        this.generations = generations;
        this.migrations = migrations;
        this.jdbc = jdbc;
    }

    @GetMapping("/v1/pipelines")
    List<Map<String, Object>> list() {
        return props.pipelines().stream().map(this::summary).toList();
    }

    @GetMapping("/v1/pipelines/{name}")
    Map<String, Object> detail(@PathVariable String name) {
        PipelineConfig p = props.require(name);
        Map<String, Object> out = summary(p);
        out.put("generations", generations.all(name));
        Map<String, Object> freshness = jdbc.queryForMap("""
                SELECT percentile_cont(0.5) WITHIN GROUP (ORDER BY freshness_ms) AS p50,
                       percentile_cont(0.99) WITHIN GROUP (ORDER BY freshness_ms) AS p99
                FROM recent_change WHERE pipeline = ? AND NOT snapshot AND applied_at > now() - interval '15 minutes'""", name);
        Map<String, Object> f = new LinkedHashMap<>();
        f.put("p50Ms", freshness.get("p50"));
        f.put("p99Ms", freshness.get("p99"));
        out.put("freshness", f);
        Map<String, Long> outcomes = new LinkedHashMap<>();
        jdbc.query("""
                SELECT outcome, count(*) FROM recent_change WHERE pipeline = ? AND applied_at > now() - interval '15 minutes'
                GROUP BY outcome ORDER BY outcome""", rs -> {
            outcomes.put(rs.getString(1), rs.getLong(2));
        }, name);
        out.put("outcomes", outcomes);
        Object migration = null;
        try {
            migration = migrations.progress(name);
        } catch (NotFoundException e) {
            // no migration running
        }
        out.put("migration", migration);
        return out;
    }

    @GetMapping("/v1/pipelines/{name}/changes")
    CursorPage<Map<String, Object>> changes(@PathVariable String name, @RequestParam(required = false) String cursor,
                                            @RequestParam(required = false) Integer limit) {
        props.require(name);
        int n = CursorPage.clampLimit(limit);
        List<Map<String, Object>> rows = jdbc.queryForList("""
                SELECT id, generation_id AS "generationId", row_key AS key, outcome, chunks_embedded AS "chunksEmbedded",
                       cache_hits AS "cacheHits", freshness_ms AS "freshnessMs", snapshot, applied_at AS "appliedAt"
                FROM recent_change WHERE pipeline = ? AND id < ? ORDER BY id DESC LIMIT ?""", name, CursorPage.before(cursor), n + 1);
        return CursorPage.of(rows, n, r -> ((Number) r.get("id")).longValue(), r -> r);
    }

    @GetMapping("/v1/pipelines/{name}/snapshots")
    List<Map<String, Object>> snapshots(@PathVariable String name) {
        props.require(name);
        return jdbc.queryForList("""
                SELECT id, kind, cardinality(keys) AS "keyCount", generation_id AS "generationId", reason, status,
                       created_at AS "createdAt", started_at AS "startedAt", finished_at AS "finishedAt"
                FROM snapshot_request WHERE pipeline = ? ORDER BY id DESC LIMIT 100""", name);
    }

    /** RUNNING, STARTING, FAILED or MISSING; a FAILED task means no changes are flowing and the slot is retaining WAL. */
    private String connectorState(String pipeline) {
        try {
            return connectors.state(pipeline).name();
        } catch (RuntimeException e) {
            return "UNREACHABLE";
        }
    }

    private Map<String, Object> summary(PipelineConfig p) {
        Map<String, Object> out = new LinkedHashMap<>();
        out.put("name", p.name());
        out.put("table", p.table());
        List<Map<String, Object>> row = jdbc.queryForList(
                "SELECT status, blocked_reason, embeds_saved FROM pipeline WHERE name = ?", p.name());
        out.put("status", row.isEmpty() ? null : row.getFirst().get("status"));
        out.put("blockedReason", row.isEmpty() ? null : row.getFirst().get("blocked_reason"));
        out.put("connector", connectorState(p.name()));
        Optional<Generation> live = generations.live(p.name());
        out.put("liveGeneration", live.map(Generation::number).orElse(null));
        // how far behind the source the newest applied change was
        List<Long> last = jdbc.queryForList(
                "SELECT freshness_ms FROM recent_change WHERE pipeline = ? AND NOT snapshot ORDER BY id DESC LIMIT 1", Long.class, p.name());
        out.put("lagSeconds", last.isEmpty() ? 0.0 : last.getFirst() / 1000.0);
        out.put("rows", live.map(g -> jdbc.queryForObject(
                "SELECT count(*) FROM row_state WHERE generation_id = ? AND NOT deleted", Long.class, g.id())).orElse(0L));
        // ponytail: exact count(*) per request; switch to pg_class.reltuples if chunk tables get large
        out.put("chunks", live.map(g -> jdbc.queryForObject("SELECT count(*) FROM " + ChunkStore.table(g), Long.class)).orElse(0L));
        out.put("embedCallsSaved", row.isEmpty() ? 0L : row.getFirst().get("embeds_saved"));
        out.put("failedOpen", jdbc.queryForObject(
                "SELECT count(*) FROM failed_change WHERE pipeline = ? AND status = 'OPEN'", Long.class, p.name()));
        out.put("textStale", live.map(g -> jdbc.queryForObject(
                "SELECT count(*) FROM row_state WHERE generation_id = ? AND text_stale AND NOT deleted", Long.class, g.id())).orElse(0L));
        return out;
    }
}
