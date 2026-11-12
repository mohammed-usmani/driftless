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
    }
}
