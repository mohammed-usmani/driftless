package dev.driftless.server.reconcile;

import dev.driftless.server.api.ConflictException;
import dev.driftless.server.api.NotFoundException;
import dev.driftless.server.apply.Prepared;
import dev.driftless.server.apply.Preparer;
import dev.driftless.server.change.ChangeEvent;
import dev.driftless.server.change.Op;
import dev.driftless.server.change.RowKeys;
import dev.driftless.server.change.Version;
import dev.driftless.server.config.DriftlessProperties;
import dev.driftless.server.config.PipelineConfig;
import dev.driftless.server.generation.Generation;
import dev.driftless.server.generation.GenerationRepository;
import dev.driftless.server.generation.RowState;
import dev.driftless.server.generation.RowStateRepository;
import dev.driftless.server.source.SnapshotQueue;
import dev.driftless.server.source.SourceReader;
import java.time.Clock;
import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;

/**
 * Samples the source and the live index and reports rows that disagree. A finding is reported only if it
 * still holds after the grace window and the source row didn't change in between, so a change that reaches the
 * index within the window is never reported. One delayed longer (a consumer backlog, a provider outage) can be;
 * repairing it is harmless, since the versioned apply drops whatever is older.
 */
@Service
public class Reconciler {

    private static final Logger log = LoggerFactory.getLogger(Reconciler.class);

    /** {@code source} is the source row's doc+meta hash when found (null for a leak); {@code version} the index version. */
    private record Finding(DriftReport.Kind kind, String detail, String source, Version version) { }

    private final DriftlessProperties props;
    private final GenerationRepository generations;
    private final RowStateRepository rows;
    private final SourceReader reader;
    private final Preparer preparer;
    private final DriftRepository drift;
    private final SnapshotQueue snapshots;
    private final LeakRepair leaks;
    private final JdbcTemplate jdbc;
    private final dev.driftless.server.ops.DriftlessMetrics metrics;
    private final Clock clock;

    public Reconciler(DriftlessProperties props, GenerationRepository generations, RowStateRepository rows, SourceReader reader,
                      Preparer preparer, DriftRepository drift, SnapshotQueue snapshots, LeakRepair leaks, JdbcTemplate jdbc,
                      dev.driftless.server.ops.DriftlessMetrics metrics, Clock clock) {
        this.props = props;
        this.generations = generations;
        this.rows = rows;
        this.reader = reader;
        this.preparer = preparer;
        this.drift = drift;
        this.snapshots = snapshots;
        this.leaks = leaks;
        this.jdbc = jdbc;
        this.metrics = metrics;
        this.clock = clock;
    }

    public DriftReport run(String pipeline) {
        PipelineConfig p = config(pipeline);
        Generation g = generations.live(pipeline).orElseThrow(() -> new ConflictException("pipeline " + pipeline + " has no live generation"));
        Instant started = clock.instant();
        int n = props.reconciler().sampleSize();

        Map<String, Finding> found = new LinkedHashMap<>();
        List<Map<String, Object>> sample = reader.sample(p, n);
        for (Map<String, Object> row : sample) {
            String key = RowKeys.of(row.get(p.key()));
            forward(p, g, key, row).ifPresent(f -> found.put(key, f));
        }
        List<String> indexed = jdbc.queryForList(
                "SELECT row_key FROM row_state WHERE generation_id = ? AND NOT deleted ORDER BY random() LIMIT ?", String.class, g.id(), n);
        Map<String, Map<String, Object>> present = reader.rows(p, indexed);
        for (String key : indexed) {
            if (!present.containsKey(key)) {
                rows.find(g.id(), key).ifPresent(s -> found.put(key, new Finding(DriftReport.Kind.LEAK, "absent from source", null, s.version())));
            }
        }

        if (!found.isEmpty()) {
            sleep(props.reconciler().graceSeconds());
            Map<String, Map<String, Object>> now = reader.rows(p, found.keySet());
            found.entrySet().removeIf(e -> !stillDrifting(p, g, e.getKey(), e.getValue(), now.get(e.getKey())));
        }

        List<DriftReport.Item> items = found.entrySet().stream()
                .map(e -> new DriftReport.Item(e.getKey(), e.getValue().kind(), e.getValue().detail(), false)).toList();
        DriftReport report = new DriftReport(0, pipeline, g.id(), started, clock.instant(), sample.size(), indexed.size(),
                count(items, DriftReport.Kind.MISSING), count(items, DriftReport.Kind.MISMATCH), count(items, DriftReport.Kind.LEAK), 0);
        long id = drift.save(report, items);
        items.forEach(i -> metrics.drift(pipeline, i.kind().name()));
        if (!items.isEmpty()) {
            log.warn("drift in {}: {} missing, {} mismatched, {} leaked", pipeline, report.missing(), report.mismatched(), report.leaked());
            if (props.reconciler().autoRepair()) {
                repair(id);
            }
        }
        return drift.find(id).orElseThrow();
    }

    /** Missing and stale rows are re-read through the stream (a keys snapshot); leaks get a synthetic delete. */
    public void repair(long reportId) {
        DriftReport report = drift.find(reportId).orElseThrow(() -> new NotFoundException("no drift report " + reportId));
        PipelineConfig p = config(report.pipeline());
        List<String> reread = new ArrayList<>();
        List<String> leaked = new ArrayList<>();
        for (DriftReport.Item i : drift.items(reportId)) {
            if (!i.repaired()) {
                (i.kind() == DriftReport.Kind.LEAK ? leaked : reread).add(i.key());
            }
        }
        if (!reread.isEmpty()) {
            snapshots.request(p.name(), SnapshotQueue.Kind.KEYS, reread, null, SnapshotQueue.Reason.REPAIR);
        }
        int deleted = leaked.isEmpty() ? 0 : leaks.delete(p, leaked);
        List<String> all = new ArrayList<>(reread);
        all.addAll(leaked);
        drift.markRepaired(reportId, all, reread.size() + deleted);
    }

    private Optional<Finding> forward(PipelineConfig p, Generation g, String key, Map<String, Object> row) {
        Prepared prep = preparer.prepare(g, new ChangeEvent(p.name(), Op.UPSERT, key, row, new Version(0, 0), clock.instant(), false, false));
        if (prep.renderError() != null) {
            return Optional.empty(); // a failed change, already reported as one
        }
        String source = prep.docHash() + prep.metaHash();
        Optional<RowState> s = rows.find(g.id(), key);
        if (s.isEmpty() || s.get().deleted()) {
            return Optional.of(new Finding(DriftReport.Kind.MISSING, "in source, not indexed", source, null));
        }
        boolean text = !prep.docHash().equals(s.get().docHash());
        boolean meta = !prep.metaHash().equals(s.get().metaHash());
        if (!text && !meta) {
            return Optional.empty();
        }
        return Optional.of(new Finding(DriftReport.Kind.MISMATCH, text && meta ? "text and metadata differ" : text ? "text differs" : "metadata differs",
                source, s.get().version()));
    }

    private boolean stillDrifting(PipelineConfig p, Generation g, String key, Finding first, Map<String, Object> row) {
        if (first.kind() == DriftReport.Kind.LEAK) {
            Optional<RowState> s = rows.find(g.id(), key);
            return row == null && s.isPresent() && !s.get().deleted() && s.get().version().equals(first.version());
        }
        if (row == null) {
            return false; // deleted meanwhile
        }
        Optional<Finding> again = forward(p, g, key, row);
        return again.isPresent() && again.get().kind() == first.kind() && Objects.equals(again.get().source(), first.source());
    }

    private static int count(List<DriftReport.Item> items, DriftReport.Kind kind) {
        return (int) items.stream().filter(i -> i.kind() == kind).count();
    }

    private PipelineConfig config(String pipeline) {
        PipelineConfig p = props.pipeline(pipeline);
        if (p == null) {
            throw new NotFoundException("no pipeline " + pipeline);
        }
        return p;
    }

    private static void sleep(int seconds) {
        try {
            Thread.sleep(seconds * 1000L);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("interrupted during the grace window", e);
        }
    }
}
