package dev.driftless.server.api;

import dev.driftless.server.apply.FailedChangeService;
import dev.driftless.server.config.DriftlessProperties;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.springframework.http.HttpStatus;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.bind.annotation.RestController;

@RestController
class FailedChangeController {

    private static final Set<String> STATUSES = Set.of("OPEN", "RETRYING", "RESOLVED");

    private final DriftlessProperties props;
    private final FailedChangeService failed;
    private final JdbcTemplate jdbc;

    FailedChangeController(DriftlessProperties props, FailedChangeService failed, JdbcTemplate jdbc) {
        this.props = props;
        this.failed = failed;
        this.jdbc = jdbc;
    }

    @GetMapping("/v1/pipelines/{name}/failed-changes")
    CursorPage<Map<String, Object>> list(@PathVariable String name, @RequestParam(required = false) String status,
                                         @RequestParam(required = false) String cursor, @RequestParam(required = false) Integer limit) {
        props.require(name);
        if (status != null && !STATUSES.contains(status)) {
            throw new ApiException(HttpStatus.BAD_REQUEST, "status must be one of " + STATUSES);
        }
        int n = CursorPage.clampLimit(limit);
        List<Map<String, Object>> rows = jdbc.queryForList("""
                SELECT id, row_key AS "rowKey", version_major || '/' || version_minor AS version, reason, payload, status,
                       created_at AS "createdAt", resolved_at AS "resolvedAt"
                FROM failed_change WHERE pipeline = ? AND (?::text IS NULL OR status = ?) AND id < ?
                ORDER BY id DESC LIMIT ?""", name, status, status, CursorPage.before(cursor), n + 1);
        return CursorPage.of(rows, n, r -> ((Number) r.get("id")).longValue(), r -> r);
    }

    /** Re-reads the row through the stream; the change resolves when that apply succeeds. */
    @PostMapping("/v1/failed-changes/{id}/retry")
    @ResponseStatus(HttpStatus.ACCEPTED)
    Map<String, String> retry(@PathVariable long id) {
        failed.retry(id);
        return Map.of("status", "RETRYING");
    }
}
