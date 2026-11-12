package dev.driftless.server.source;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import dev.driftless.server.apply.FailedChangeService;
import dev.driftless.server.apply.Prepared;
import dev.driftless.server.apply.Preparer;
import dev.driftless.server.change.ChangeEvent;
import dev.driftless.server.generation.Generation;
import dev.driftless.server.generation.GenerationRepository;
import dev.driftless.server.support.IntegrationTest;
import dev.driftless.server.support.SourceDb;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.config.BeanPostProcessor;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Import;
import org.springframework.test.context.TestPropertySource;

@Import(SnapshotQueueIT.Failing.class)
@TestPropertySource(properties = {
        "driftless.pipelines[0].name=sq_articles",
        "driftless.pipelines[0].table=public.sq_articles",
        "driftless.pipelines[0].key=id",
        "driftless.pipelines[0].text={title}",
        "driftless.pipelines[0].embedding.provider=fake",
        "driftless.pipelines[0].embedding.model=fake-8",
        "driftless.pipelines[0].embedding.dimensions=8",
})
class SnapshotQueueIT extends IntegrationTest {

    /** Keys whose text fails to render, for as long as they're in the set. */
    static final Set<String> BROKEN = ConcurrentHashMap.newKeySet();

    @TestConfiguration(proxyBeanMethods = false)
    static class Failing {
        @Bean
        static BeanPostProcessor failingPreparer() {
            return new BeanPostProcessor() {
                @Override
                public Object postProcessAfterInitialization(Object bean, String name) {
                    if (!(bean instanceof Preparer)) {
                        return bean;
                    }
                    return new Preparer() {
                        @Override
                        public Prepared prepare(Generation g, ChangeEvent e) {
                            if (e.key() != null && BROKEN.contains(e.key())) {
                                return new Prepared(null, null, null, null, null, "forced failure for " + e.key());
                            }
                            return super.prepare(g, e);
                        }
                    };
                }
            };
        }
    }

    @Autowired GenerationRepository generations;
    @Autowired SnapshotQueue queue;
    @Autowired FailedChangeService failedChanges;

    @BeforeAll
    static void seed() {
        SourceDb.createArticles(sourceJdbc(), "sq_articles");
        List<Object[]> rows = new ArrayList<>();
        for (int i = 1; i <= 500; i++) {
            rows.add(new Object[] {i, "Article " + i});
        }
        sourceJdbc().batchUpdate("INSERT INTO sq_articles (id, title) VALUES (?, ?)", rows);
    }

    private long liveRows() {
        return jdbc.queryForObject("""
                SELECT count(*) FROM row_state r JOIN pipeline p ON p.live_generation_id = r.generation_id
                WHERE p.name = 'sq_articles' AND NOT r.deleted""", Long.class);
    }

    private List<String> statuses() {
        return jdbc.queryForList("SELECT status FROM snapshot_request WHERE pipeline = 'sq_articles' ORDER BY id", String.class);
    }

    @Test
    void bootstrapBackfillsExistingRows() {
        await().atMost(Duration.ofMinutes(2)).until(() -> liveRows() >= 500);
        await().atMost(Duration.ofMinutes(1)).until(() -> jdbc.queryForObject(
                "SELECT status FROM snapshot_request WHERE pipeline = 'sq_articles' AND reason = 'BACKFILL'", String.class)
                .equals("COMPLETED"));
    }

    @Test
    void requestsRunOneAtATimePerPipeline() {
        await().atMost(Duration.ofMinutes(2)).until(() -> !statuses().contains("QUEUED") && !statuses().contains("RUNNING"));
        int before = statuses().size();
        for (int i = 0; i < 3; i++) {
            queue.request("sq_articles", SnapshotQueue.Kind.FULL, List.of(), null, SnapshotQueue.Reason.REPAIR);
        }
        long deadline = System.currentTimeMillis() + 120_000;
        while (System.currentTimeMillis() < deadline) {
            List<String> s = statuses();
            assertThat(s.stream().filter("RUNNING"::equals).count()).isLessThanOrEqualTo(1);
            if (s.size() == before + 3 && s.stream().allMatch("COMPLETED"::equals)) {
                return;
            }
            try {
                Thread.sleep(50);
            } catch (InterruptedException e) {
                throw new IllegalStateException(e);
            }
        }
        throw new AssertionError("snapshots did not all complete: " + statuses());
    }

    @Test
    void retryOfFailedChangeResolvesIt() {
        BROKEN.add("7001");
        sourceJdbc().update("INSERT INTO sq_articles (id, title) VALUES (7001, 'Broken for now')");
        await().atMost(Duration.ofMinutes(1)).until(() -> jdbc.queryForObject(
                "SELECT count(*) FROM failed_change WHERE pipeline = 'sq_articles' AND row_key = '7001' AND status = 'OPEN'", Long.class) == 1);
        long id = jdbc.queryForObject("SELECT id FROM failed_change WHERE pipeline = 'sq_articles' AND row_key = '7001'", Long.class);

        BROKEN.remove("7001");
        failedChanges.retry(id);
        assertThat(jdbc.queryForObject("SELECT status FROM failed_change WHERE id = ?", String.class, id)).isEqualTo("RETRYING");
        await().atMost(Duration.ofMinutes(2)).until(() -> jdbc.queryForObject(
                "SELECT status FROM failed_change WHERE id = ?", String.class, id).equals("RESOLVED"));
        String table = generations.live("sq_articles").orElseThrow().chunkTable();
        assertThat(jdbc.queryForObject("SELECT count(*) FROM " + table + " WHERE row_key = '7001'", Long.class)).isEqualTo(1);
    }

    @Test
    void keysRequestSplitsIntoSignalsOf200() {
        List<String> keys = new ArrayList<>();
        for (int i = 0; i < 450; i++) {
            keys.add(Integer.toString(100_000 + i));
        }
        List<Long> ids = queue.request("sq_articles", SnapshotQueue.Kind.KEYS, keys, null, SnapshotQueue.Reason.REPAIR);
        assertThat(ids).hasSize(3);
        assertThat(jdbc.queryForList("SELECT cardinality(keys) FROM snapshot_request WHERE id = ANY (?) ORDER BY id", Integer.class,
                (Object) ids.toArray(Long[]::new))).containsExactly(200, 200, 50);
    }
}
