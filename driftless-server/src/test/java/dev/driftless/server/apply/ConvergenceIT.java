package dev.driftless.server.apply;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import dev.driftless.server.change.Hashes;
import dev.driftless.server.change.RowKeys;
import dev.driftless.server.change.TextRenderer;
import dev.driftless.server.config.DriftlessProperties;
import dev.driftless.server.config.PipelineConfig;
import dev.driftless.server.generation.Generation;
import dev.driftless.server.generation.GenerationRepository;
import dev.driftless.server.source.SnapshotQueue;
import dev.driftless.server.support.IntegrationTest;
import dev.driftless.server.support.RandomWriters;
import dev.driftless.server.support.SourceDb;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.listener.MessageListenerContainer;
import org.springframework.test.context.TestPropertySource;

/**
 * The end-to-end guarantee: whatever happens on the source and however records are redelivered, the live
 * index ends up exactly what the source implies, with no orphans and no resurrections.
 */
@TestPropertySource(properties = {
        "driftless.pipelines[0].name=cv_articles",
        "driftless.pipelines[0].table=public.cv_articles",
        "driftless.pipelines[0].key=id",
        "driftless.pipelines[0].text={title}\\n\\n{body}",
        "driftless.pipelines[0].metadata=category,tenant_id,acl",
        "driftless.pipelines[0].embedding.provider=fake",
        "driftless.pipelines[0].embedding.model=fake-8",
        "driftless.pipelines[0].embedding.dimensions=8",
        "driftless.pipelines[0].chunking.max-chars=1000",
        "driftless.pipelines[0].chunking.overlap-chars=100",
        "driftless.pipelines[1].name=cv_trunc",
        "driftless.pipelines[1].table=public.cv_trunc",
        "driftless.pipelines[1].key=id",
        "driftless.pipelines[1].text={title}\\n\\n{body}",
        "driftless.pipelines[1].metadata=category,tenant_id,acl",
        "driftless.pipelines[1].embedding.provider=fake",
        "driftless.pipelines[1].embedding.model=fake-8",
        "driftless.pipelines[1].embedding.dimensions=8",
})
class ConvergenceIT extends IntegrationTest {

    @Autowired DriftlessProperties props;
    @Autowired GenerationRepository generations;
    @Autowired SnapshotQueue snapshots;
    @Autowired KafkaListenerEndpointRegistry listeners;

    @BeforeAll
    static void tables() {
        SourceDb.createArticles(sourceJdbc(), "cv_articles");
        SourceDb.createArticles(sourceJdbc(), "cv_trunc");
    }

    @Test
    void indexConvergesUnderRedeliveryAndSnapshots() throws Exception {
        awaitSlotActive("cv_articles");
        long seed = Long.getLong("convergence.seed", ThreadLocalRandom.current().nextLong());
        Thread writers = Thread.ofPlatform().start(() -> RandomWriters.run(sourceJdbcUrl(), POSTGRES.getUsername(),
                POSTGRES.getPassword(), "cv_articles", 4, Duration.ofSeconds(30), 200, seed));
        MessageListenerContainer changes = listeners.getListenerContainer("changes");
        Thread.sleep(7_000);
        changes.stop();
        Thread.sleep(2_000);
        changes.start();
        Thread.sleep(5_000);
        snapshots.request("cv_articles", SnapshotQueue.Kind.FULL, List.of(), null, SnapshotQueue.Reason.REPAIR);
        Thread.sleep(5_000);
        changes.stop();
        Thread.sleep(1_000);
        changes.start();
        writers.join();

        assertConverged("cv_articles", seed);
    }

    @Test
    void indexConvergesAcrossATruncate() throws Exception {
        awaitSlotActive("cv_trunc");
        long seed = Long.getLong("convergence.seed", ThreadLocalRandom.current().nextLong());
        Thread writers = Thread.ofPlatform().start(() -> RandomWriters.run(sourceJdbcUrl(), POSTGRES.getUsername(),
                POSTGRES.getPassword(), "cv_trunc", 4, Duration.ofSeconds(15), 100, seed));
        Thread.sleep(7_000);
        sourceJdbc().execute("TRUNCATE cv_trunc");
        writers.join();

        assertConverged("cv_trunc", seed);
    }

    private void assertConverged(String pipeline, long seed) {
        PipelineConfig p = props.pipeline(pipeline);
        await("convergence of " + pipeline + " (seed " + seed + ")").atMost(Duration.ofMinutes(3)).pollInterval(Duration.ofSeconds(2))
                .untilAsserted(() -> assertThat(differences(p)).isEmpty());
        assertThat(jdbc.queryForObject("SELECT count(*) FROM failed_change WHERE pipeline = ?", Long.class, pipeline)).isZero();
    }

    /** Everything about the live generation that doesn't match the source, as readable lines. */
    private List<String> differences(PipelineConfig p) {
        Generation g = generations.live(p.name()).orElseThrow();
        Map<String, Map<String, Object>> source = new HashMap<>();
        for (Map<String, Object> row : sourceJdbc().queryForList("SELECT * FROM " + p.table())) {
            source.put(RowKeys.of(row.get("id")), row);
        }
        Map<String, Map<String, Object>> states = new HashMap<>();
        for (Map<String, Object> s : jdbc.queryForList("SELECT * FROM row_state WHERE generation_id = ?", g.id())) {
            states.put((String) s.get("row_key"), s);
        }
        Map<String, List<Integer>> chunks = new HashMap<>();
        for (Map<String, Object> c : jdbc.queryForList("SELECT row_key, chunk_no FROM " + g.chunkTable() + " ORDER BY row_key, chunk_no")) {
            chunks.computeIfAbsent((String) c.get("row_key"), k -> new ArrayList<>()).add((Integer) c.get("chunk_no"));
        }

        List<String> diffs = new ArrayList<>();
        source.forEach((key, row) -> {
            Map<String, Object> s = states.get(key);
            if (s == null || (Boolean) s.get("deleted")) {
                diffs.add(key + ": in source but " + (s == null ? "not indexed" : "deleted in index"));
                return;
            }
            String doc = Hashes.sha256Hex(TextRenderer.render(p.text(), row));
            Map<String, Object> meta = new LinkedHashMap<>();
            p.metadata().forEach(c -> meta.put(c, row.get(c)));
            if (!doc.equals(s.get("doc_hash"))) {
                diffs.add(key + ": text differs");
            }
            if (!Hashes.metadata(meta).equals(s.get("meta_hash"))) {
                diffs.add(key + ": metadata differs");
            }
            int n = (Integer) s.get("chunk_count");
            List<Integer> expected = java.util.stream.IntStream.range(0, n).boxed().toList();
            if (!expected.equals(chunks.getOrDefault(key, List.of()))) {
                diffs.add(key + ": chunks " + chunks.getOrDefault(key, List.of()) + " but chunk_count " + n);
            }
        });
        states.forEach((key, s) -> {
            if (!source.containsKey(key) && !(Boolean) s.get("deleted")) {
                diffs.add(key + ": deleted in source but live in index");
            }
        });
        chunks.keySet().forEach(key -> {
            if (!source.containsKey(key)) {
                diffs.add(key + ": chunks left for a deleted row");
            }
        });
        return diffs;
    }
}
