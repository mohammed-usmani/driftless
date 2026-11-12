package dev.driftless.server.apply;

import dev.driftless.server.source.SnapshotQueue;
import java.util.List;
import java.util.Map;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;

@Service
public class FailedChangeService {

    private final JdbcTemplate jdbc;
    private final SnapshotQueue snapshots;

    public FailedChangeService(JdbcTemplate jdbc, SnapshotQueue snapshots) {
        this.jdbc = jdbc;
        this.snapshots = snapshots;
    }

    /**
     * Re-reads the row through the stream (a one-row snapshot) instead of from the source directly, so the
     * retry can never race a newer change. Resolved when an apply for that key next succeeds.
     */
    public void retry(long id) {
        List<Map<String, Object>> rows = jdbc.queryForList(
                "UPDATE failed_change SET status = 'RETRYING' WHERE id = ? AND status <> 'RESOLVED' RETURNING pipeline, row_key", id);
        if (rows.isEmpty()) {
            throw new IllegalArgumentException("no open failed change " + id);
        }
        String key = (String) rows.getFirst().get("row_key");
        if (key == null) {
            throw new IllegalArgumentException("failed change " + id + " has no row key; it can only be resolved by a later change");
        }
        snapshots.request((String) rows.getFirst().get("pipeline"), SnapshotQueue.Kind.KEYS, List.of(key), null, SnapshotQueue.Reason.RETRY);
    }
}
