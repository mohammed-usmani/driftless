package dev.driftless.server.api;

import dev.driftless.server.config.DriftlessProperties;
import dev.driftless.server.reconcile.DriftReport;
import dev.driftless.server.reconcile.DriftRepository;
import dev.driftless.server.reconcile.Reconciler;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.core.task.TaskExecutor;
import org.springframework.http.HttpStatus;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.bind.annotation.RestController;

@RestController
class DriftController {

    private static final Logger log = LoggerFactory.getLogger(DriftController.class);

    private final DriftlessProperties props;
    private final DriftRepository drift;
    private final Reconciler reconciler;
    private final TaskExecutor executor;
    private final JdbcTemplate jdbc;

    DriftController(DriftlessProperties props, DriftRepository drift, Reconciler reconciler,
                    @Qualifier("applicationTaskExecutor") TaskExecutor executor, JdbcTemplate jdbc) {
        this.props = props;
        this.drift = drift;
        this.reconciler = reconciler;
        this.executor = executor;
        this.jdbc = jdbc;
    }

    @GetMapping("/v1/pipelines/{name}/drift-reports")
    CursorPage<Map<String, Object>> list(@PathVariable String name, @RequestParam(required = false) String cursor,
                                         @RequestParam(required = false) Integer limit) {
        props.require(name);
        int n = CursorPage.clampLimit(limit);
        List<Map<String, Object>> rows = jdbc.queryForList("""
                SELECT id, generation_id AS "generationId", started_at AS "startedAt", finished_at AS "finishedAt",
                       sampled_source AS "sampledSource", sampled_index AS "sampledIndex", missing, mismatched, leaked, repaired
                FROM drift_report WHERE pipeline = ? AND id < ? ORDER BY id DESC LIMIT ?""", name, CursorPage.before(cursor), n + 1);
        return CursorPage.of(rows, n, r -> ((Number) r.get("id")).longValue(), r -> r);
    }

    @GetMapping("/v1/drift-reports/{id}")
    Map<String, Object> get(@PathVariable long id) {
        DriftReport r = drift.find(id).orElseThrow(() -> new NotFoundException("no drift report " + id));
        Map<String, Object> out = new LinkedHashMap<>();
        out.put("id", r.id());
        out.put("pipeline", r.pipeline());
        out.put("generationId", r.generationId());
        out.put("startedAt", r.startedAt());
        out.put("finishedAt", r.finishedAt());
        out.put("sampledSource", r.sampledSource());
        out.put("sampledIndex", r.sampledIndex());
        out.put("missing", r.missing());
        out.put("mismatched", r.mismatched());
        out.put("leaked", r.leaked());
        out.put("repaired", r.repaired());
        out.put("items", drift.items(id));
        return out;
    }

    /** Runs in the background: a run waits out the grace window. The report appears in the list when done. */
    @PostMapping("/v1/pipelines/{name}/reconcile")
    @ResponseStatus(HttpStatus.ACCEPTED)
    Map<String, String> reconcile(@PathVariable String name) {
        props.require(name);
        executor.execute(() -> {
            try {
                reconciler.run(name);
            } catch (RuntimeException e) {
                log.warn("reconcile of {} failed: {}", name, e.getMessage());
            }
        });
        return Map.of("status", "started");
    }

    @PostMapping("/v1/drift-reports/{id}/repair")
    @ResponseStatus(HttpStatus.ACCEPTED)
    Map<String, String> repair(@PathVariable long id) {
        drift.find(id).orElseThrow(() -> new NotFoundException("no drift report " + id));
        executor.execute(() -> {
            try {
                reconciler.repair(id);
            } catch (RuntimeException e) {
                log.warn("repair of drift report {} failed: {}", id, e.getMessage());
            }
        });
        return Map.of("status", "started");
    }
}
