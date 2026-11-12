package dev.driftless.server.apply;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import dev.driftless.server.config.DriftlessProperties;
import dev.driftless.server.config.PipelineConfig;
import dev.driftless.server.generation.GenerationRepository;
import dev.driftless.server.source.SnapshotQueue;
import dev.driftless.server.support.Convergence;
import dev.driftless.server.support.IntegrationTest;
import dev.driftless.server.support.RandomWriters;
import dev.driftless.server.support.SourceDb;
import java.time.Duration;
import java.util.List;
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
                .untilAsserted(() -> assertThat(Convergence.differences(jdbc, sourceJdbc(), p, generations.live(p.name()).orElseThrow())).isEmpty());
        assertThat(jdbc.queryForObject("SELECT count(*) FROM failed_change WHERE pipeline = ?", Long.class, pipeline)).isZero();
    }
}
