package dev.driftless.server.generation;

import dev.driftless.server.config.PipelineConfig;
import dev.driftless.server.embed.EmbeddingSpec;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.annotation.Transactional;

@Repository
public class GenerationRepository {

    private static final String COLUMNS = """
            id, pipeline, number, provider, model, dimensions, template, max_chars, overlap_chars,
            metadata_columns, config_hash, status, chunk_table""";

    private final JdbcTemplate jdbc;

    public GenerationRepository(JdbcTemplate jdbc) {
        this.jdbc = jdbc;
    }

    public void upsertPipeline(PipelineConfig p, PipelineStatus status, String blockedReason) {
        jdbc.update("""
                INSERT INTO pipeline (name, table_name, status, blocked_reason) VALUES (?, ?, ?, ?)
                ON CONFLICT (name) DO UPDATE SET table_name = excluded.table_name, status = excluded.status,
                    blocked_reason = excluded.blocked_reason, updated_at = now()""",
                p.name(), p.table(), status.name(), blockedReason);
    }

    public PipelineStatus status(String pipeline) {
        return PipelineStatus.valueOf(jdbc.queryForObject("SELECT status FROM pipeline WHERE name = ?", String.class, pipeline));
    }

    public String blockedReason(String pipeline) {
        return jdbc.queryForObject("SELECT blocked_reason FROM pipeline WHERE name = ?", String.class, pipeline);
    }

    /** A new generation numbered after the pipeline's last one; the caller creates its chunk table. */
    @Transactional
    public Generation create(PipelineConfig p, GenerationStatus status) {
        jdbc.queryForObject("SELECT name FROM pipeline WHERE name = ? FOR UPDATE", String.class, p.name());
        int number = jdbc.queryForObject("SELECT coalesce(max(number), 0) + 1 FROM generation WHERE pipeline = ?", Integer.class, p.name());
        String table = "chunks_" + p.name() + "_g" + number;
        long id = jdbc.queryForObject("""
                INSERT INTO generation (pipeline, number, provider, model, dimensions, template, max_chars, overlap_chars,
                                        metadata_columns, config_hash, status, chunk_table, live_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CASE WHEN ? = 'LIVE' THEN now() END) RETURNING id""",
                Long.class, p.name(), number, p.embedding().provider(), p.embedding().model(), p.embedding().dimensions(),
                p.text(), p.chunking().maxChars(), p.chunking().overlapChars(), p.metadata().toArray(String[]::new),
                p.configHash(), status.name(), table, status.name());
        if (status == GenerationStatus.LIVE) {
            jdbc.update("UPDATE pipeline SET live_generation_id = ? WHERE name = ?", id, p.name());
        }
        return find(id).orElseThrow();
    }

    public Optional<Generation> find(long id) {
        return jdbc.query("SELECT " + COLUMNS + " FROM generation WHERE id = ?", GenerationRepository::map, id).stream().findFirst();
    }

    public Optional<Generation> live(String pipeline) {
        return jdbc.query("SELECT " + COLUMNS + " FROM generation WHERE pipeline = ? AND status = 'LIVE'",
                GenerationRepository::map, pipeline).stream().findFirst();
    }

    /** Generations that receive live changes, oldest first. */
    public List<Generation> targets(String pipeline) {
        return jdbc.query("SELECT " + COLUMNS + " FROM generation WHERE pipeline = ? AND status <> 'DROPPED' ORDER BY number",
                GenerationRepository::map, pipeline);
    }

    public List<Generation> all(String pipeline) {
        return jdbc.query("SELECT " + COLUMNS + " FROM generation WHERE pipeline = ? ORDER BY number",
                GenerationRepository::map, pipeline);
    }

    public void setStatus(long id, GenerationStatus status) {
        jdbc.update("""
                UPDATE generation SET status = ?,
                    ready_at = CASE WHEN ? = 'READY' THEN now() ELSE ready_at END,
                    live_at = CASE WHEN ? = 'LIVE' THEN now() ELSE live_at END,
                    dropped_at = CASE WHEN ? = 'DROPPED' THEN now() ELSE dropped_at END
                WHERE id = ?""", status.name(), status.name(), status.name(), status.name(), id);
    }

    public void setLive(String pipeline, long generationId) {
        jdbc.update("UPDATE pipeline SET live_generation_id = ?, updated_at = now() WHERE name = ?", generationId, pipeline);
    }

    private static Generation map(ResultSet rs, int i) throws SQLException {
        return new Generation(rs.getLong("id"), rs.getString("pipeline"), rs.getInt("number"),
                new EmbeddingSpec(rs.getString("provider"), rs.getString("model"), rs.getInt("dimensions")),
                rs.getString("template"), rs.getInt("max_chars"), rs.getInt("overlap_chars"),
                Arrays.asList((String[]) rs.getArray("metadata_columns").getArray()), rs.getString("config_hash"),
                GenerationStatus.valueOf(rs.getString("status")), rs.getString("chunk_table"));
    }
}
