package dev.driftless.server.embed;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;

/** Vectors by (model, chunk hash), shared by every pipeline and generation. */
@Component
public class EmbeddingCache {

    private final JdbcTemplate jdbc;

    public EmbeddingCache(JdbcTemplate jdbc) {
        this.jdbc = jdbc;
    }

    /** Found vectors by hash; marks them used so retention keeps them. */
    public Map<String, float[]> getAll(String model, Collection<String> hashes) {
        Map<String, float[]> out = new HashMap<>();
        if (hashes.isEmpty()) {
            return out;
        }
        String[] arr = hashes.toArray(String[]::new);
        jdbc.query("""
                UPDATE embedding_cache SET last_used_at = now()
                WHERE model = ? AND chunk_hash = ANY (?)
                RETURNING chunk_hash, embedding::text""",
                rs -> { out.put(rs.getString(1), Vectors.parse(rs.getString(2))); },
                model, arr);
        return out;
    }

    public void putAll(String model, Map<String, float[]> vectors) {
        jdbc.batchUpdate("""
                INSERT INTO embedding_cache (model, chunk_hash, embedding, last_used_at)
                VALUES (?, ?, ?::vector, now())
                ON CONFLICT (model, chunk_hash) DO UPDATE SET embedding = excluded.embedding, last_used_at = now()""",
                vectors.entrySet().stream()
                        .map(e -> new Object[] {model, e.getKey(), Vectors.toText(e.getValue())}).toList());
    }
}
