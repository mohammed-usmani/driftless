package dev.driftless.server.apply;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import dev.driftless.server.generation.GenerationRepository;
import dev.driftless.server.support.ControllableFake;
import dev.driftless.server.support.IntegrationTest;
import dev.driftless.server.support.SourceDb;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Import;
import org.springframework.test.context.TestPropertySource;

@Import(ControllableFake.Config.class)
@TestPropertySource(properties = {
        "driftless.pipelines[0].name=cc_articles",
        "driftless.pipelines[0].table=public.cc_articles",
        "driftless.pipelines[0].key=id",
        "driftless.pipelines[0].text={title}\\n\\n{body}",
        "driftless.pipelines[0].metadata=category,tenant_id,acl",
        "driftless.pipelines[0].embedding.provider=fake",
        "driftless.pipelines[0].embedding.model=fake-8",
        "driftless.pipelines[0].embedding.dimensions=8",
        "driftless.pipelines[1].name=cc_other",
        "driftless.pipelines[1].table=public.cc_other",
        "driftless.pipelines[1].key=id",
        "driftless.pipelines[1].text={title}\\n\\n{body}",
        "driftless.pipelines[1].embedding.provider=fake",
        "driftless.pipelines[1].embedding.model=fake-8",
        "driftless.pipelines[1].embedding.dimensions=8",
})
class ChangeConsumerIT extends IntegrationTest {

    private static final Duration WAIT = Duration.ofSeconds(60);

    @Autowired GenerationRepository generations;
    @Autowired dev.driftless.server.source.KafkaOffsets offsets;
    @Autowired dev.driftless.server.source.ConnectorManager connectors;
    @Autowired @org.springframework.beans.factory.annotation.Qualifier("fake") dev.driftless.server.embed.EmbeddingProvider provider;
    ControllableFake fake;

    @BeforeAll
    static void tables() {
        SourceDb.createArticles(sourceJdbc(), "cc_articles");
        SourceDb.createArticles(sourceJdbc(), "cc_other");
    }

    @BeforeEach
    void slots() {
        fake = (ControllableFake) provider;
        awaitSlotActive("cc_articles");
        awaitSlotActive("cc_other");
        fake.downUntil = Instant.MIN;
    }

    private long id = System.nanoTime() % 1_000_000_000;

    private long insert(String table, String title, String body) {
        long k = ++id;
        sourceJdbc().update("INSERT INTO " + table + " (id, title, body, category, acl) VALUES (?, ?, ?, 'c', ARRAY['u1'])", k, title, body);
        return k;
    }

    private String chunkTable(String pipeline) {
        return generations.live(pipeline).orElseThrow().chunkTable();
    }

    private long chunks(String pipeline, long key) {
        return jdbc.queryForObject("SELECT count(*) FROM " + chunkTable(pipeline) + " WHERE row_key = ?", Long.class, Long.toString(key));
    }

    private String lastOutcome(String pipeline, long key) {
        List<String> o = jdbc.queryForList("SELECT outcome FROM recent_change WHERE pipeline = ? AND row_key = ? ORDER BY id DESC LIMIT 1",
                String.class, pipeline, Long.toString(key));
        return o.isEmpty() ? null : o.getFirst();
    }

    @Test
    void topicOfAPipelineThisInstanceDoesNotRunIsLeftUnconsumed() throws Exception {
        // during a rolling deploy that adds a pipeline, an old instance must not take the new topic's partitions and ack them away
        String topic = "driftless.cc_newer.public.cc_newer";
        var config = java.util.Map.<String, Object>of("bootstrap.servers", KAFKA.getBootstrapServers());
        try (var admin = org.apache.kafka.clients.admin.Admin.create(config)) {
            admin.createTopics(List.of(new org.apache.kafka.clients.admin.NewTopic(topic, 1, (short) 1))).all().get();
        }
        try (var producer = new org.apache.kafka.clients.producer.KafkaProducer<String, String>(config,
                new org.apache.kafka.common.serialization.StringSerializer(), new org.apache.kafka.common.serialization.StringSerializer())) {
            producer.send(new org.apache.kafka.clients.producer.ProducerRecord<>(topic, "{\"id\":1}", "{\"op\":\"c\"}")).get();
        }
        Thread.sleep(15_000); // three metadata refreshes: a pattern subscription would have picked it up by now
        assertThat(offsets.committed(topic)).as("offsets committed on another pipeline's topic").isEmpty();
    }

    @Test
    void insertBecomesSearchableRows() {
        long k = insert("cc_articles", "Reset a password", "Open settings.");
        await().atMost(WAIT).until(() -> chunks("cc_articles", k) == 1);
    }

    @Test
    void updateTouchingOnlyUpdatedAtIsSkipped() {
        long k = insert("cc_articles", "Title", "Body");
        await().atMost(WAIT).until(() -> chunks("cc_articles", k) == 1);
        int calls = fake.texts.get();
        sourceJdbc().update("UPDATE cc_articles SET updated_at = now() + interval '1 hour' WHERE id = ?", k);
        await().atMost(WAIT).until(() -> "SKIP".equals(lastOutcome("cc_articles", k)));
        assertThat(fake.texts.get()).isEqualTo(calls);
    }

    @Test
    void deleteRemovesChunks() {
        long k = insert("cc_articles", "Title", "Body");
        await().atMost(WAIT).until(() -> chunks("cc_articles", k) == 1);
        sourceJdbc().update("DELETE FROM cc_articles WHERE id = ?", k);
        await().atMost(WAIT).until(() -> chunks("cc_articles", k) == 0);
        assertThat(lastOutcome("cc_articles", k)).isEqualTo("DELETE");
    }

    @Test
    void aclChangeIsMetadataOnly() {
        long k = insert("cc_articles", "Title", "Body");
        await().atMost(WAIT).until(() -> chunks("cc_articles", k) == 1);
        int calls = fake.texts.get();
        sourceJdbc().update("UPDATE cc_articles SET acl = ARRAY['u2'] WHERE id = ?", k);
        await().atMost(WAIT).until(() -> "METADATA_ONLY".equals(lastOutcome("cc_articles", k)));
        assertThat(jdbc.queryForObject("SELECT metadata->'acl'->>0 FROM " + chunkTable("cc_articles") + " WHERE row_key = ?",
                String.class, Long.toString(k))).isEqualTo("u2");
        assertThat(fake.texts.get()).isEqualTo(calls);
    }

    @Test
    void titleOnlyUpdateOfToastedBodyKeepsBody() {
        StringBuilder body = new StringBuilder();
        java.util.Random rnd = new java.util.Random(1);
        while (body.length() < 50_000) {
            body.append(Long.toString(rnd.nextLong(), 36)).append(' ');
        }
        long k = insert("cc_articles", "First", body.toString());
        await().atMost(WAIT).until(() -> chunks("cc_articles", k) > 1);
        sourceJdbc().update("UPDATE cc_articles SET title = 'Second' WHERE id = ?", k);
        await().atMost(WAIT).until(() -> jdbc.queryForObject("SELECT text FROM " + chunkTable("cc_articles")
                + " WHERE row_key = ? AND chunk_no = 0", String.class, Long.toString(k)).startsWith("Second"));
        String all = String.join("", jdbc.queryForList("SELECT text FROM " + chunkTable("cc_articles")
                + " WHERE row_key = ? ORDER BY chunk_no", String.class, Long.toString(k)));
        assertThat(all).contains(body.substring(body.length() - 200)).doesNotContain("__debezium_unavailable_value");
    }

    @Test
    void providerOutageDelaysButLosesNothing() {
        fake.downUntil = Instant.now().plusSeconds(5);
        long a = insert("cc_articles", "During outage A", "x");
        long b = insert("cc_articles", "During outage B", "y");
        await().atMost(Duration.ofSeconds(90)).until(() -> chunks("cc_articles", a) == 1 && chunks("cc_articles", b) == 1);
        assertThat(jdbc.queryForObject("SELECT count(*) FROM failed_change WHERE pipeline = 'cc_articles' AND row_key IN (?, ?)", Long.class,
                Long.toString(a), Long.toString(b))).isZero();
    }

    @Test
    void providerOutageStillAppliesDeletesAndAclChanges() {
        long revoked = insert("cc_articles", "Revoked", "x");
        long deleted = insert("cc_articles", "Deleted", "y");
        await().atMost(WAIT).until(() -> chunks("cc_articles", revoked) == 1 && chunks("cc_articles", deleted) == 1);
        fake.downUntil = Instant.now().plusSeconds(120);
        try {
            long waiting = insert("cc_articles", "Needs an embedding", "z");
            sourceJdbc().update("UPDATE cc_articles SET acl = ARRAY['u9'] WHERE id = ?", revoked);
            sourceJdbc().update("DELETE FROM cc_articles WHERE id = ?", deleted);
            await().atMost(Duration.ofSeconds(45)).until(() -> chunks("cc_articles", deleted) == 0 && "u9".equals(jdbc.queryForObject(
                    "SELECT metadata->'acl'->>0 FROM " + chunkTable("cc_articles") + " WHERE row_key = ?", String.class, Long.toString(revoked))));
            assertThat(chunks("cc_articles", waiting)).isZero();
            fake.downUntil = Instant.MIN;
            await().atMost(WAIT).until(() -> chunks("cc_articles", waiting) == 1);
        } finally {
            fake.downUntil = Instant.MIN;
        }
    }

    @Test
    void rowTheProviderRejectsBecomesAFailedChangeAndTheRestApply() {
        long rejected = insert("cc_articles", "PROVIDER-REJECTS this one", "x");
        long fine = insert("cc_articles", "An ordinary row", "y");
        await().atMost(WAIT).until(() -> chunks("cc_articles", fine) == 1);
        await().atMost(WAIT).until(() -> jdbc.queryForObject(
                "SELECT count(*) FROM failed_change WHERE pipeline = 'cc_articles' AND row_key = ? AND reason LIKE '%rejected%'",
                Long.class, Long.toString(rejected)) == 1);
        assertThat(chunks("cc_articles", rejected)).isZero();
    }

    @Test
    void rowOverAMegabyteIsIndexedAndTheStreamKeepsFlowing() {
        // Kafka's default 1 MB record limit; an update carries the before image too (REPLICA IDENTITY FULL), so double it
        String body = "large ".repeat(250_000);
        long big = insert("cc_articles", "Large", body);
        sourceJdbc().update("UPDATE cc_articles SET title = 'Large, edited' WHERE id = ?", big);
        long after = insert("cc_articles", "After the large row", "z");
        await().atMost(Duration.ofSeconds(120)).until(() -> chunks("cc_articles", after) == 1 && jdbc.queryForObject(
                "SELECT count(*) FROM " + chunkTable("cc_articles") + " WHERE row_key = ? AND text LIKE 'Large, edited%'", Long.class,
                Long.toString(big)) == 1);
        assertThat(connectors.state("cc_articles")).isEqualTo(dev.driftless.server.source.ConnectorManager.State.RUNNING);
    }

    @Test
    void droppedTemplateColumnBecomesFailedChange() {
        sourceJdbc().execute("ALTER TABLE cc_other DROP COLUMN body");
        long k = ++id;
        sourceJdbc().update("INSERT INTO cc_other (id, title) VALUES (?, 'no body')", k);
        await().atMost(WAIT).until(() -> jdbc.queryForObject(
                "SELECT count(*) FROM failed_change WHERE pipeline = 'cc_other' AND row_key = ? AND status = 'OPEN'",
                Long.class, Long.toString(k)) == 1);
        assertThat(jdbc.queryForObject("SELECT reason FROM failed_change WHERE pipeline = 'cc_other' AND row_key = ?",
                String.class, Long.toString(k))).contains("body");
        long other = insert("cc_articles", "Still flowing", "yes");
        await().atMost(WAIT).until(() -> chunks("cc_articles", other) == 1);
    }
}
