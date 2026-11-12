package dev.driftless.server.generation;

import dev.driftless.server.change.Version;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.Optional;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

@Repository
public class RowStateRepository {

    private final JdbcTemplate jdbc;

    public RowStateRepository(JdbcTemplate jdbc) {
        this.jdbc = jdbc;
    }

    public Optional<RowState> find(long generationId, String key) {
        return jdbc.query("SELECT * FROM row_state WHERE generation_id = ? AND row_key = ?", RowStateRepository::map,
                generationId, key).stream().findFirst();
    }

    /**
     * Serialises appliers of one (generation, row) until the transaction ends, then reads its state. An
     * advisory lock, because a row lock can't protect a row that doesn't exist yet. Call inside a transaction.
     */
    public Optional<RowState> lock(long generationId, String key) {
        jdbc.queryForList("SELECT pg_advisory_xact_lock(hashtextextended(? || ':' || ?, 0))", generationId, key);
        return find(generationId, key);
    }

    public void upsert(RowState s) {
        jdbc.update("""
                INSERT INTO row_state (generation_id, row_key, version_major, version_minor, doc_hash, meta_hash,
                                       chunk_count, deleted, text_stale, committed_at, applied_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, now())
                ON CONFLICT (generation_id, row_key) DO UPDATE SET version_major = excluded.version_major,
                    version_minor = excluded.version_minor, doc_hash = excluded.doc_hash, meta_hash = excluded.meta_hash,
                    chunk_count = excluded.chunk_count, deleted = excluded.deleted, text_stale = excluded.text_stale,
                    committed_at = excluded.committed_at, applied_at = now()""",
                s.generationId(), s.key(), s.version().major(), s.version().minor(), s.docHash(), s.metaHash(),
                s.chunkCount(), s.deleted(), s.textStale(), s.committedAt() == null ? null : Timestamp.from(s.committedAt()));
    }

    /** Rows indexed and not deleted. */
    public long countLive(long generationId) {
        return jdbc.queryForObject("SELECT count(*) FROM row_state WHERE generation_id = ? AND NOT deleted", Long.class, generationId);
    }

    static RowState map(ResultSet rs, int i) throws SQLException {
        Timestamp committed = rs.getTimestamp("committed_at");
        return new RowState(rs.getLong("generation_id"), rs.getString("row_key"),
                new Version(rs.getLong("version_major"), rs.getLong("version_minor")), rs.getString("doc_hash"),
                rs.getString("meta_hash"), rs.getInt("chunk_count"), rs.getBoolean("deleted"), rs.getBoolean("text_stale"),
                committed == null ? null : committed.toInstant());
    }
}
