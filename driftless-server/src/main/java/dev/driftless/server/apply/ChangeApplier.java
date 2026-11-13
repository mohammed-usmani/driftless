package dev.driftless.server.apply;

import dev.driftless.server.change.ChangeEvent;
import dev.driftless.server.embed.EmbeddingService;
import dev.driftless.server.generation.Chunk;
import dev.driftless.server.generation.ChunkStore;
import dev.driftless.server.generation.Generation;
import dev.driftless.server.generation.RowState;
import dev.driftless.server.generation.RowStateRepository;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

/**
 * Applies one change to one generation in its own short transaction. The caller classifies and embeds
 * without any lock; here the row is locked and classified again, so a newer change that landed meanwhile
 * always wins.
 */
@Component
public class ChangeApplier {

    private final RowStateRepository rows;
    private final ChunkStore chunks;
    private final EmbeddingService embeddings;
    private final JdbcTemplate jdbc;

    public ChangeApplier(RowStateRepository rows, ChunkStore chunks, EmbeddingService embeddings, JdbcTemplate jdbc) {
        this.rows = rows;
        this.chunks = chunks;
        this.embeddings = embeddings;
        this.jdbc = jdbc;
    }

    /** {@code vectors} lines up with {@code p.chunks()}; it may be null when the caller expected no embedding. */
    @Transactional(propagation = Propagation.REQUIRES_NEW)
    public Outcome apply(Generation g, ChangeEvent e, Prepared p, List<float[]> vectors) {
        if (e.op() == dev.driftless.server.change.Op.TRUNCATE) {
            truncate(g, e);
            return Outcome.TRUNCATE;
        }
        if (olderThanLastTruncate(g, e)) {
            return Outcome.STALE;
        }
        Optional<RowState> current = rows.lock(g.id(), e.key());
        Outcome outcome = Classifier.classify(e, p, current);
        switch (outcome) {
            case STALE, FAILED -> { }
            case EMBED -> {
                List<float[]> v = vectors;
                if (v == null || v.size() != p.chunks().size()) {
                    // the row changed between the unlocked classification and now; rare, so embed here
                    v = embeddings.embed(g.embedding(), p.chunks()).vectors();
                }
                List<Chunk> list = new ArrayList<>(p.chunks().size());
                for (int i = 0; i < p.chunks().size(); i++) {
                    String text = p.chunks().get(i);
                    list.add(new Chunk(i, text, dev.driftless.server.change.Hashes.sha256Hex(text), v.get(i)));
                }
                chunks.replace(g, e.key(), list, p.metadata(), e.version());
                rows.upsert(new RowState(g.id(), e.key(), e.version(), p.docHash(), p.metaHash(), list.size(), false, false, e.committedAt()));
            }
            case METADATA_ONLY -> {
                RowState c = current.orElseThrow();
                chunks.updateMetadata(g, e.key(), p.metadata());
                boolean stale = p.renderError() != null || c.textStale();
                rows.upsert(new RowState(g.id(), e.key(), e.version(), c.docHash(), p.metaHash(), c.chunkCount(), false, stale, e.committedAt()));
            }
            case SKIP -> {
                RowState c = current.orElseThrow();
                rows.upsert(new RowState(g.id(), e.key(), e.version(), c.docHash(), c.metaHash(), c.chunkCount(), false, c.textStale(), e.committedAt()));
            }
            case DELETE -> {
                chunks.deleteRow(g, e.key());
                rows.upsert(new RowState(g.id(), e.key(), e.version(), null, null, 0, true, false, e.committedAt()));
            }
            case TRUNCATE -> throw new IllegalStateException("truncate handled above");
        }
        return outcome;
    }

    /**
     * Every apply holds this generation's truncate lock shared until it commits, and a truncate holds it exclusively,
     * so an apply either finishes before the truncate (which then removes it) or sees the truncate's version.
     */
    private boolean olderThanLastTruncate(Generation g, ChangeEvent e) {
        return Boolean.TRUE.equals(jdbc.queryForObject("""
                SELECT (?, ?) < (truncated_major, truncated_minor)
                FROM generation, pg_advisory_xact_lock_shared(hashtextextended(? || ':truncate', 0))
                WHERE id = ?""", Boolean.class, e.version().major(), e.version().minor(), g.id(), g.id()));
    }

    /** Removes every row applied before the truncate; rows applied after it (a later insert) stay. */
    private void truncate(Generation g, ChangeEvent e) {
        jdbc.queryForList("SELECT pg_advisory_xact_lock(hashtextextended(? || ':truncate', 0))", g.id());
        jdbc.update("UPDATE generation SET truncated_major = ?, truncated_minor = ? WHERE id = ? AND (truncated_major, truncated_minor) < (?, ?)",
                e.version().major(), e.version().minor(), g.id(), e.version().major(), e.version().minor());
        String older = "(version_major, version_minor) < (?, ?)";
        jdbc.update("DELETE FROM " + g.chunkTable() + " c USING row_state r WHERE r.generation_id = ? AND r.row_key = c.row_key AND "
                + older.replace("version", "r.version"), g.id(), e.version().major(), e.version().minor());
        jdbc.update("UPDATE row_state SET deleted = true, chunk_count = 0, doc_hash = NULL, meta_hash = NULL, text_stale = false, "
                + "version_major = ?, version_minor = ?, committed_at = ?, applied_at = now() WHERE generation_id = ? AND " + older,
                e.version().major(), e.version().minor(), Timestamp.from(e.committedAt()), g.id(), e.version().major(), e.version().minor());
    }
}
