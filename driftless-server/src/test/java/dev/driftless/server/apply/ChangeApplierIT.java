package dev.driftless.server.apply;

import static org.assertj.core.api.Assertions.assertThat;

import dev.driftless.server.change.ChangeEvent;
import dev.driftless.server.change.Op;
import dev.driftless.server.change.Version;
import dev.driftless.server.generation.ChunkStore;
import dev.driftless.server.generation.Generation;
import dev.driftless.server.generation.GenerationRepository;
import dev.driftless.server.generation.GenerationStatus;
import dev.driftless.server.generation.PipelineStatus;
import dev.driftless.server.generation.RowState;
import dev.driftless.server.generation.RowStateRepository;
import dev.driftless.server.config.PipelineConfig;
import dev.driftless.server.support.IntegrationTest;
import dev.driftless.server.support.Pipelines;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;

class ChangeApplierIT extends IntegrationTest {

    @Autowired GenerationRepository generations;
    @Autowired ChunkStore chunks;
    @Autowired RowStateRepository rows;
    @Autowired Preparer preparer;
    @Autowired ChangeApplier applier;

    private Generation g;

    @BeforeEach
    void generation() {
        PipelineConfig p = Pipelines.articles("ap_" + System.nanoTime() % 1_000_000);
        generations.upsertPipeline(p, PipelineStatus.ACTIVE, null);
        g = generations.create(p, GenerationStatus.LIVE);
        chunks.createTable(g);
    }

    private static Map<String, Object> row(String title, String body, List<String> acl) {
        Map<String, Object> r = new HashMap<>();
        r.put("id", 1L);
        r.put("title", title);
        r.put("body", body);
        r.put("category", "c");
        r.put("tenant_id", "t");
        r.put("acl", acl);
        return r;
    }

    private ChangeEvent upsert(String key, long v, Map<String, Object> row) {
        return new ChangeEvent(g.pipeline(), Op.UPSERT, key, row, new Version(v, 0), Instant.now(), false, false);
    }

    private Outcome apply(ChangeEvent e) {
        Prepared p = preparer.prepare(g, e);
        List<float[]> vectors = null;
        if (p.chunks() != null) {
            vectors = new ArrayList<>();
            for (int i = 0; i < p.chunks().size(); i++) {
                vectors.add(new float[] {1, 0, 0, 0, 0, 0, 0, i});
            }
        }
        return applier.apply(g, e, p, vectors);
    }

    private Outcome delete(String key, long v) {
        return apply(new ChangeEvent(g.pipeline(), Op.DELETE, key, null, new Version(v, 0), Instant.now(), false, false));
    }

    private long chunkCount(String key) {
        return jdbc.queryForObject("SELECT count(*) FROM " + g.chunkTable() + " WHERE row_key = ?", Long.class, key);
    }

    private RowState state(String key) {
        return rows.find(g.id(), key).orElseThrow();
    }

    @Test
    void appliesInsertUpdateDelete() {
        assertThat(apply(upsert("1", 1, row("A", "body", List.of("u1"))))).isEqualTo(Outcome.EMBED);
        assertThat(chunkCount("1")).isEqualTo(1);
        assertThat(apply(upsert("1", 2, row("B", "body", List.of("u1"))))).isEqualTo(Outcome.EMBED);
        assertThat(jdbc.queryForObject("SELECT text FROM " + g.chunkTable() + " WHERE row_key = '1'", String.class)).startsWith("B");
        assertThat(delete("1", 3)).isEqualTo(Outcome.DELETE);
        assertThat(chunkCount("1")).isZero();
        assertThat(state("1").deleted()).isTrue();
    }

    @Test
    void olderVersionAfterNewerIsStale() {
        apply(upsert("1", 5, row("new", "b", List.of())));
        assertThat(apply(upsert("1", 3, row("old", "b", List.of())))).isEqualTo(Outcome.STALE);
        assertThat(state("1").version()).isEqualTo(new Version(5, 0));
        assertThat(jdbc.queryForObject("SELECT text FROM " + g.chunkTable() + " WHERE row_key = '1'", String.class)).startsWith("new");
    }

    @Test
    void deleteThenOlderUpsertDoesNotResurrect() {
        apply(upsert("1", 1, row("A", "b", List.of())));
        delete("1", 5);
        assertThat(apply(upsert("1", 3, row("A", "b", List.of())))).isEqualTo(Outcome.STALE);
        assertThat(chunkCount("1")).isZero();
        assertThat(state("1").deleted()).isTrue();
    }

    @Test
    void chunkCountShrinkRemovesOrphans() {
        String longBody = ("word ".repeat(100) + "\n\n").repeat(20); // ~10k chars, 2000-char chunks
        apply(upsert("1", 1, row("A", longBody, List.of())));
        long before = chunkCount("1");
        assertThat(before).isGreaterThan(3);
        apply(upsert("1", 2, row("A", "short", List.of())));
        assertThat(chunkCount("1")).isEqualTo(1);
        assertThat(state("1").chunkCount()).isEqualTo(1);
    }

    @Test
    void aclOnlyChangeUpdatesMetadataWithoutVectors() {
        apply(upsert("1", 1, row("A", "b", List.of("u1"))));
        ChangeEvent e = upsert("1", 2, row("A", "b", List.of("u2")));
        assertThat(applier.apply(g, e, preparer.prepare(g, e), null)).isEqualTo(Outcome.METADATA_ONLY);
        assertThat(jdbc.queryForObject("SELECT metadata->'acl'->>0 FROM " + g.chunkTable() + " WHERE row_key = '1'", String.class))
                .isEqualTo("u2");
        assertThat(state("1").version()).isEqualTo(new Version(2, 0));
    }

    @Test
    void unchangedRowIsSkippedButItsVersionAdvances() {
        apply(upsert("1", 1, row("A", "b", List.of())));
        assertThat(apply(upsert("1", 2, row("A", "b", List.of())))).isEqualTo(Outcome.SKIP);
        assertThat(state("1").version()).isEqualTo(new Version(2, 0));
    }

    @Test
    void renderErrorAppliesAclAndMarksTextStale() {
        apply(upsert("1", 1, row("A", "b", List.of("u1"))));
        Map<String, Object> broken = row("A", "__debezium_unavailable_value", List.of("u2"));
        assertThat(apply(upsert("1", 2, broken))).isEqualTo(Outcome.METADATA_ONLY);
        assertThat(state("1").textStale()).isTrue();
        assertThat(jdbc.queryForObject("SELECT metadata->'acl'->>0 FROM " + g.chunkTable() + " WHERE row_key = '1'", String.class))
                .isEqualTo("u2");
        assertThat(jdbc.queryForObject("SELECT text FROM " + g.chunkTable() + " WHERE row_key = '1'", String.class)).startsWith("A");

        assertThat(apply(upsert("1", 3, row("A", "b", List.of("u2"))))).isEqualTo(Outcome.EMBED);
        assertThat(state("1").textStale()).isFalse();
    }

    @Test
    void truncateTombstonesAllRowsAndBlocksOlderUpserts() {
        apply(upsert("1", 1, row("A", "b", List.of())));
        apply(upsert("2", 2, row("B", "b", List.of())));
        ChangeEvent truncate = new ChangeEvent(g.pipeline(), Op.TRUNCATE, null, null, new Version(10, 0), Instant.now(), false, false);
        assertThat(applier.apply(g, truncate, Prepared.NONE, null)).isEqualTo(Outcome.TRUNCATE);
        assertThat(jdbc.queryForObject("SELECT count(*) FROM " + g.chunkTable(), Long.class)).isZero();
        assertThat(state("1").deleted()).isTrue();
        assertThat(state("2").version()).isEqualTo(new Version(10, 0));
        assertThat(apply(upsert("1", 5, row("A", "b", List.of())))).isEqualTo(Outcome.STALE);
        assertThat(apply(upsert("1", 11, row("A", "b", List.of())))).isEqualTo(Outcome.EMBED);
    }

    @Test
    void truncateKeepsRowsAppliedAfterIt() {
        apply(upsert("1", 20, row("A", "b", List.of())));
        ChangeEvent truncate = new ChangeEvent(g.pipeline(), Op.TRUNCATE, null, null, new Version(10, 0), Instant.now(), false, false);
        applier.apply(g, truncate, Prepared.NONE, null);
        assertThat(chunkCount("1")).isEqualTo(1);
        assertThat(state("1").deleted()).isFalse();
    }

    @Test
    void olderInsertArrivingAfterATruncateIsStale() {
        // truncate records have no key, so they can be applied while an older insert still waits on another partition
        ChangeEvent truncate = new ChangeEvent(g.pipeline(), Op.TRUNCATE, null, null, new Version(10, 0), Instant.now(), false, false);
        applier.apply(g, truncate, Prepared.NONE, null);
        assertThat(apply(upsert("7", 5, row("Before the truncate", "b", List.of())))).isEqualTo(Outcome.STALE);
        assertThat(chunkCount("7")).isZero();
        assertThat(apply(upsert("8", 11, row("After the truncate", "b", List.of())))).isEqualTo(Outcome.EMBED);
    }

    @Test
    void emptyTextIndexesZeroChunks() {
        assertThat(apply(upsert("1", 1, row(null, null, List.of())))).isEqualTo(Outcome.EMBED);
        assertThat(chunkCount("1")).isZero();
        assertThat(state("1").chunkCount()).isZero();
        assertThat(state("1").deleted()).isFalse();
    }
}
