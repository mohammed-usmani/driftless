package dev.driftless.server.reconcile;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import dev.driftless.server.change.RowKeys;
import dev.driftless.server.change.Version;
import dev.driftless.server.config.DriftlessProperties;
import dev.driftless.server.config.PipelineConfig;
import dev.driftless.server.generation.Generation;
import dev.driftless.server.generation.GenerationRepository;
import dev.driftless.server.source.ConnectorManager;
import dev.driftless.server.support.IntegrationTest;
import dev.driftless.server.support.KafkaProbe;
import dev.driftless.server.support.SourceDb;
import java.time.Duration;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.TestPropertySource;

@TestPropertySource(properties = {
        "driftless.reconciler.grace-seconds=3",
        "driftless.pipelines[0].name=lr_articles",
        "driftless.pipelines[0].table=public.lr_articles",
        "driftless.pipelines[0].key=id",
        "driftless.pipelines[0].text={title}",
        "driftless.pipelines[0].embedding.provider=fake",
        "driftless.pipelines[0].embedding.model=fake-8",
        "driftless.pipelines[0].embedding.dimensions=8",
        "driftless.pipelines[1].name=lr_uuid",
        "driftless.pipelines[1].table=public.lr_uuid",
        "driftless.pipelines[1].key=id",
        "driftless.pipelines[1].text={title}",
        "driftless.pipelines[1].embedding.provider=fake",
        "driftless.pipelines[1].embedding.model=fake-8",
        "driftless.pipelines[1].embedding.dimensions=8",
})
class LeakRepairIT extends IntegrationTest {

    @Autowired LeakRepair leakRepair;
    @Autowired DriftlessProperties props;
    @Autowired ConnectorManager connectors;
    @Autowired GenerationRepository generations;

    @BeforeAll
    static void tables() {
        SourceDb.createArticles(sourceJdbc(), "lr_articles");
        SourceDb.create(sourceJdbc(), "lr_uuid", "uuid", "");
    }

    @BeforeEach
    void slots() {
        awaitSlotActive("lr_articles");
        awaitSlotActive("lr_uuid");
    }

    private void assertSameKeyAndPartition(String pipeline, Object id) {
        PipelineConfig p = props.pipeline(pipeline);
        try (KafkaProbe probe = new KafkaProbe(KAFKA.getBootstrapServers(), connectors.topic(p))) {
            String title = "key bytes " + id;
            sourceJdbc().update("INSERT INTO " + p.table() + " (id, title) VALUES (?, ?)", id, title);
            ConsumerRecord<String, String> real = probe.await(r -> r.value() != null && r.value().contains(title), Duration.ofSeconds(60));
            String ours = RowKeys.debeziumKeyJson("id", id);
            assertThat(ours).isEqualTo(real.key());
            // version 0/0 is older than anything applied, so this synthetic delete is dropped as stale
            int partition = leakRepair.publish(p, RowKeys.of(id), new Version(0, 0));
            assertThat(partition).isEqualTo(real.partition());
        }
    }

    @Test
    void syntheticKeyBytesMatchDebezium() {
        assertSameKeyAndPartition("lr_articles", 4_200_000_017L);
        assertSameKeyAndPartition("lr_articles", 9L);
        assertSameKeyAndPartition("lr_uuid", UUID.fromString("6F9619FF-8B86-D011-B42D-00C04FC964FF"));
    }

    @Test
    void reinsertAfterLeakCheckWins() throws Exception {
        PipelineConfig p = props.pipeline("lr_articles");
        Generation g = generations.live("lr_articles").orElseThrow();
        String key = "77";
        // a leak: indexed, absent from the source
        jdbc.update("""
                INSERT INTO row_state (generation_id, row_key, version_major, version_minor, doc_hash, meta_hash, chunk_count,
                                       deleted, text_stale, committed_at, applied_at) VALUES (?, ?, 1, 0, 'x', 'y', 1, false, false, now(), now())""",
                g.id(), key);
        jdbc.update("INSERT INTO " + g.chunkTable() + " (row_key, chunk_no, text, chunk_hash, embedding, metadata, version_major, version_minor) "
                + "VALUES (?, 0, 'leaked', 'h', '[1,0,0,0,0,0,0,0]', '{}', 1, 0)", key);

        CompletableFuture<Integer> repair = CompletableFuture.supplyAsync(() -> leakRepair.delete(p, List.of(key)));
        Thread.sleep(1_000);
        sourceJdbc().update("INSERT INTO lr_articles (id, title) VALUES (77, 'back again')");
        repair.join();

        await().atMost(Duration.ofMinutes(1)).until(() -> "back again".equals(jdbc.queryForList(
                "SELECT text FROM " + g.chunkTable() + " WHERE row_key = ?", String.class, key).stream().findFirst().orElse(null)));
        Thread.sleep(3_000);
        assertThat(jdbc.queryForObject("SELECT deleted FROM row_state WHERE generation_id = ? AND row_key = ?", Boolean.class, g.id(), key)).isFalse();
        assertThat(jdbc.queryForObject("SELECT count(*) FROM " + g.chunkTable() + " WHERE row_key = ?", Long.class, key)).isEqualTo(1);
    }
}
