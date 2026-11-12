package dev.driftless.server.reconcile;

import java.sql.Timestamp;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.annotation.Transactional;

@Repository
public class DriftRepository {

    private final JdbcTemplate jdbc;

    public DriftRepository(JdbcTemplate jdbc) {
        this.jdbc = jdbc;
    }

    @Transactional
    public long save(DriftReport r, List<DriftReport.Item> items) {
        long id = jdbc.queryForObject("""
                INSERT INTO drift_report (pipeline, generation_id, started_at, finished_at, sampled_source, sampled_index,
                                          missing, mismatched, leaked)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?) RETURNING id""", Long.class,
                r.pipeline(), r.generationId(), Timestamp.from(r.startedAt()), Timestamp.from(r.finishedAt()),
                r.sampledSource(), r.sampledIndex(), r.missing(), r.mismatched(), r.leaked());
        jdbc.batchUpdate("INSERT INTO drift_item (report_id, row_key, kind, detail) VALUES (?, ?, ?, ?)",
                items.stream().map(i -> new Object[] {id, i.key(), i.kind().name(), i.detail()}).toList());
        return id;
    }

    public Optional<DriftReport> find(long id) {
        return jdbc.query("SELECT * FROM drift_report WHERE id = ?", (rs, i) -> new DriftReport(rs.getLong("id"),
                rs.getString("pipeline"), rs.getLong("generation_id"), rs.getTimestamp("started_at").toInstant(),
                rs.getTimestamp("finished_at").toInstant(), rs.getInt("sampled_source"), rs.getInt("sampled_index"),
                rs.getInt("missing"), rs.getInt("mismatched"), rs.getInt("leaked"), rs.getInt("repaired")), id).stream().findFirst();
    }

    public List<DriftReport.Item> items(long reportId) {
        return jdbc.query("SELECT * FROM drift_item WHERE report_id = ? ORDER BY row_key", (rs, i) -> new DriftReport.Item(
                rs.getString("row_key"), DriftReport.Kind.valueOf(rs.getString("kind")), rs.getString("detail"),
                rs.getBoolean("repaired")), reportId);
    }

    @Transactional
    public void markRepaired(long reportId, Collection<String> keys, int repaired) {
        jdbc.update("UPDATE drift_item SET repaired = true WHERE report_id = ? AND row_key = ANY (?)", reportId, keys.toArray(String[]::new));
        jdbc.update("UPDATE drift_report SET repaired = repaired + ? WHERE id = ?", repaired, reportId);
    }
}
