package dev.driftless.server.apply;

import dev.driftless.server.change.Version;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;
import tools.jackson.databind.json.JsonMapper;

@Repository
public class FailedChangeRepository {

    private static final JsonMapper JSON = JsonMapper.builder().build();
    private final JdbcTemplate jdbc;

    public FailedChangeRepository(JdbcTemplate jdbc) {
        this.jdbc = jdbc;
    }

    /** A change that could not be applied; {@code row} is kept for the operator. */
    public void record(String pipeline, String key, Version version, String reason, Object row) {
        jdbc.update("""
                INSERT INTO failed_change (pipeline, row_key, version_major, version_minor, reason, payload)
                VALUES (?, ?, ?, ?, ?, ?)""",
                pipeline, key, version == null ? null : version.major(), version == null ? null : version.minor(), reason,
                row == null ? null : JSON.writeValueAsString(row));
    }

    /** A record that could not even be decoded; its raw value is kept as text. */
    public void recordUndecodable(String pipeline, String reason, String raw) {
        jdbc.update("INSERT INTO failed_change (pipeline, reason, payload) VALUES (?, ?, ?)", pipeline, reason, raw);
    }
}
