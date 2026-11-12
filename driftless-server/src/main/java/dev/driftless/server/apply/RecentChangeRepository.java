package dev.driftless.server.apply;

import java.util.List;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

@Repository
public class RecentChangeRepository {

    private final JdbcTemplate jdbc;

    public RecentChangeRepository(JdbcTemplate jdbc) {
        this.jdbc = jdbc;
    }

    public void record(List<Applied> applied) {
        jdbc.batchUpdate("""
                INSERT INTO recent_change (pipeline, generation_id, row_key, outcome, chunks_embedded, cache_hits, freshness_ms, snapshot)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
                applied.stream().map(a -> new Object[] {a.pipeline(), a.generationId(), a.key(), a.outcome().name(),
                        a.embedded(), a.cacheHits(), a.freshnessMs(), a.snapshot()}).toList());
        // saved: chunks served from cache, plus rows whose text didn't change and so needed no embedding
        long embedded = applied.stream().mapToLong(Applied::embedded).sum();
        long saved = applied.stream().mapToLong(a -> a.cacheHits()
                + (a.outcome() == Outcome.SKIP || a.outcome() == Outcome.METADATA_ONLY ? 1 : 0)).sum();
        if (!applied.isEmpty() && (embedded > 0 || saved > 0)) {
            jdbc.update("UPDATE pipeline SET chunks_embedded = chunks_embedded + ?, embeds_saved = embeds_saved + ? WHERE name = ?",
                    embedded, saved, applied.getFirst().pipeline());
        }
    }
}
