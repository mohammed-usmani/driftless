package dev.driftless.server.generation;

import dev.driftless.server.change.Version;
import dev.driftless.server.embed.Vectors;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;
import tools.jackson.databind.json.JsonMapper;

/**
 * The per-generation chunk tables. Table names come only from {@link Generation#chunkTable()}, built from a
 * validated pipeline name, and are checked again here before they reach SQL.
 */
@Repository
public class ChunkStore {

    private static final Pattern SAFE_TABLE = Pattern.compile("^chunks_[a-z][a-z0-9_]{0,40}_g[0-9]+$");
    private static final JsonMapper JSON = JsonMapper.builder().build();

    private final JdbcTemplate jdbc;

    public ChunkStore(JdbcTemplate jdbc) {
        this.jdbc = jdbc;
    }

    /** The generation's chunk table name, checked safe for SQL. */
    public static String table(Generation g) {
        if (!SAFE_TABLE.matcher(g.chunkTable()).matches()) {
            throw new IllegalStateException("unsafe chunk table name: " + g.chunkTable());
        }
        return g.chunkTable();
    }

    public void createTable(Generation g) {
        String t = table(g);
        jdbc.execute("""
                CREATE TABLE IF NOT EXISTS %s (
                    row_key       text    NOT NULL,
                    chunk_no      int     NOT NULL,
                    text          text    NOT NULL,
                    chunk_hash    text    NOT NULL,
                    embedding     vector(%d) NOT NULL,
                    metadata      jsonb   NOT NULL,
                    version_major bigint  NOT NULL,
                    version_minor bigint  NOT NULL,
                    PRIMARY KEY (row_key, chunk_no)
                )""".formatted(t, g.embedding().dimensions()));
        jdbc.execute("CREATE INDEX IF NOT EXISTS %s_embedding ON %s USING hnsw (embedding vector_cosine_ops)".formatted(t, t));
        jdbc.execute("CREATE INDEX IF NOT EXISTS %s_metadata ON %s USING gin (metadata)".formatted(t, t));
    }

    public void dropTable(Generation g) {
        jdbc.execute("DROP TABLE IF EXISTS " + table(g));
    }

    /** Upserts chunks 0..n-1 and deletes any chunk numbered n or more. */
    public void replace(Generation g, String key, List<Chunk> chunks, Map<String, Object> metadata, Version v) {
        String t = table(g);
        String meta = JSON.writeValueAsString(metadata);
        jdbc.batchUpdate("""
                INSERT INTO %s (row_key, chunk_no, text, chunk_hash, embedding, metadata, version_major, version_minor)
                VALUES (?, ?, ?, ?, ?::vector, ?::jsonb, ?, ?)
                ON CONFLICT (row_key, chunk_no) DO UPDATE SET text = excluded.text, chunk_hash = excluded.chunk_hash,
                    embedding = excluded.embedding, metadata = excluded.metadata,
                    version_major = excluded.version_major, version_minor = excluded.version_minor""".formatted(t),
                chunks.stream().map(c -> new Object[] {key, c.no(), c.text(), c.hash(), Vectors.toText(c.embedding()),
                        meta, v.major(), v.minor()}).toList());
        jdbc.update("DELETE FROM " + t + " WHERE row_key = ? AND chunk_no >= ?", key, chunks.size());
    }

    public void updateMetadata(Generation g, String key, Map<String, Object> metadata) {
        jdbc.update("UPDATE " + table(g) + " SET metadata = ?::jsonb WHERE row_key = ?", JSON.writeValueAsString(metadata), key);
    }

    public void deleteRow(Generation g, String key) {
        jdbc.update("DELETE FROM " + table(g) + " WHERE row_key = ?", key);
    }

    public void truncate(Generation g) {
        jdbc.execute("TRUNCATE " + table(g));
    }

    public long count(Generation g) {
        return jdbc.queryForObject("SELECT count(*) FROM " + table(g), Long.class);
    }
}
