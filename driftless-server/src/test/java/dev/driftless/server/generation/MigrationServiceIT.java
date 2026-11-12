package dev.driftless.server.generation;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.awaitility.Awaitility.await;

import dev.driftless.server.api.ConflictException;
import dev.driftless.server.config.DriftlessProperties;
import dev.driftless.server.config.PipelineConfig;
import dev.driftless.server.source.ConnectorManager;
import dev.driftless.server.support.Convergence;
import dev.driftless.server.support.IntegrationTest;
import dev.driftless.server.support.RandomWriters;
import dev.driftless.server.support.SourceDb;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.test.context.TestPropertySource;

/** Each test runs one whole migration of the same pipeline, so they run in order. */
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
@TestPropertySource(properties = {
        "driftless.pipelines[0].name=mg_articles",
        "driftless.pipelines[0].table=public.mg_articles",
        "driftless.pipelines[0].key=id",
        "driftless.pipelines[0].text={title}\\n\\n{body}",
        "driftless.pipelines[0].metadata=category,tenant_id,acl",
        "driftless.pipelines[0].embedding.provider=fake",
        "driftless.pipelines[0].embedding.model=fake-16",
        "driftless.pipelines[0].embedding.dimensions=16",
        "driftless.pipelines[0].chunking.max-chars=1000",
        "driftless.pipelines[0].chunking.overlap-chars=100",
})
class MigrationServiceIT extends IntegrationTest {

    private static final String P = "mg_articles";
    /** Seeded rows the random writers never touch (they write ids 1..100). */
    private static final String KNOWN = "1300";

    @Autowired MigrationService migrations;
    @Autowired GenerationRepository generations;
    @Autowired DriftlessProperties props;
    @Autowired ConnectorManager connectors;
    @Autowired KafkaListenerEndpointRegistry listeners;

    @BeforeAll
    static void seed() {
        SourceDb.createArticles(sourceJdbc(), P);
        List<Object[]> rows = new ArrayList<>();
        for (int i = 1001; i <= 1300; i++) {
            rows.add(new Object[] {i, "Seeded " + i, "body of " + i});
        }
        sourceJdbc().batchUpdate("INSERT INTO " + P + " (id, title, body) VALUES (?, ?, ?)", rows);
    }

    @BeforeEach
    void backfilled() {
        awaitSlotActive(P);
        await().atMost(Duration.ofMinutes(2)).until(() -> generations.live(P)
                .map(g -> jdbc.queryForObject("SELECT count(*) FROM row_state WHERE generation_id = ? AND row_key = ?",
                        Long.class, g.id(), KNOWN) == 1).orElse(false));
    }

    private PipelineConfig config() {
        return props.pipeline(P);
    }

    /** Stands in for an edited config: the live generation no longer matches it. */
    private void configChanged() {
        jdbc.update("UPDATE generation SET config_hash = 'edited' WHERE pipeline = ? AND status = 'LIVE'", P);
    }

    private GenerationStatus status(Generation g) {
        return generations.find(g.id()).orElseThrow().status();
    }

    private void awaitReady(Generation g) {
        await().atMost(Duration.ofMinutes(3)).until(() -> status(g) == GenerationStatus.READY);
    }

    private void assertConverged(Generation g) {
        await().atMost(Duration.ofMinutes(2)).pollInterval(Duration.ofSeconds(1))
                .untilAsserted(() -> assertThat(Convergence.differences(jdbc, sourceJdbc(), config(), generations.find(g.id()).orElseThrow())).isEmpty());
    }

    private void tableExists(Generation g, boolean exists) {
        assertThat(jdbc.queryForObject("SELECT to_regclass(?) IS NOT NULL", Boolean.class, g.chunkTable())).isEqualTo(exists);
    }

    @Test
    @Order(1)
    void startRequiresConfigChange() {
        assertThatThrownBy(() -> migrations.start(P)).isInstanceOf(ConflictException.class).hasMessageContaining("unchanged");
        configChanged();
        Generation g = migrations.start(P);
        assertThatThrownBy(() -> migrations.start(P)).isInstanceOf(ConflictException.class).hasMessageContaining("already");
        awaitReady(g);
        migrations.cutover(P);
        migrations.finalizeMigration(P);
    }

    @Test
    @Order(2)
    void migratesUnderLiveWrites() throws Exception {
        long seed = Long.getLong("convergence.seed", ThreadLocalRandom.current().nextLong());
        Thread writers = Thread.ofPlatform().start(() -> RandomWriters.run(sourceJdbcUrl(), POSTGRES.getUsername(),
                POSTGRES.getPassword(), P, 4, Duration.ofSeconds(25), 100, seed));
        AtomicReference<String> searchProblem = new AtomicReference<>();
        Thread searcher = Thread.ofPlatform().start(() -> {
            while (writers.isAlive()) {
                try {
                    Generation live = generations.live(P).orElseThrow();
                    long n = jdbc.queryForObject("SELECT count(*) FROM " + live.chunkTable() + " WHERE row_key = ?", Long.class, KNOWN);
                    if (n == 0) {
                        searchProblem.compareAndSet(null, "known row missing from live generation " + live.number());
                    }
                    Thread.sleep(200);
                } catch (InterruptedException e) {
                    return;
                } catch (RuntimeException e) {
                    searchProblem.compareAndSet(null, e.toString());
                }
            }
        });

        Thread.sleep(3_000);
        configChanged();
        Generation g = migrations.start(P);
        awaitReady(g);
        migrations.cutover(P);
        writers.join();
        searcher.join();

        assertThat(searchProblem.get()).as("search during migration (seed %d)", seed).isNull();
        assertThat(generations.live(P).orElseThrow().id()).isEqualTo(g.id());
        assertConverged(g);
        migrations.finalizeMigration(P);
    }

    @Test
    @Order(3)
    void deleteDuringMigrationNeverResurrects() {
        configChanged();
        Generation old = generations.live(P).orElseThrow();
        Generation g = migrations.start(P);
        sourceJdbc().update("DELETE FROM " + P + " WHERE id = 1002");
        awaitReady(g);
        for (Generation gen : List.of(old, g)) {
            await().atMost(Duration.ofMinutes(1)).until(() -> jdbc.queryForObject(
                    "SELECT count(*) FROM " + gen.chunkTable() + " WHERE row_key = '1002'", Long.class) == 0);
            assertThat(jdbc.queryForObject("SELECT count(*) FROM row_state WHERE generation_id = ? AND row_key = '1002' AND NOT deleted",
                    Long.class, gen.id())).isZero();
        }
        migrations.cutover(P);
        migrations.finalizeMigration(P);
    }

    @Test
    @Order(4)
    void crashMidBackfillResumes() {
        var changes = listeners.getListenerContainer("changes");
        configChanged();
        changes.stop();
        Generation g = migrations.start(P);
        await().atMost(Duration.ofMinutes(1)).until(() -> "RUNNING".equals(jdbc.queryForObject(
                "SELECT status FROM snapshot_request WHERE generation_id = ?", String.class, g.id())));
        connectors.restart(P);
        changes.start();
        awaitReady(g);
        assertConverged(g);
        migrations.cutover(P);
        migrations.finalizeMigration(P);
    }

    @Test
    @Order(5)
    void readyWaitsForTheConsumerToApplyTheSnapshot() {
        var changes = listeners.getListenerContainer("changes");
        configChanged();
        changes.stop();
        Generation g = migrations.start(P);
        await().atMost(Duration.ofMinutes(2)).until(() -> "COMPLETED".equals(jdbc.queryForObject(
                "SELECT status FROM snapshot_request WHERE generation_id = ?", String.class, g.id())));
        assertThat(status(g)).isEqualTo(GenerationStatus.BUILDING);
        changes.start();
        awaitReady(g);
        assertThat(migrations.progress(P).rows()).isEqualTo(migrations.progress(P).sourceRows());
        migrations.cutover(P);
        migrations.finalizeMigration(P);
    }

    @Test
    @Order(6)
    void rollbackRestoresOldGeneration() {
        configChanged();
        Generation old = generations.live(P).orElseThrow();
        Generation g = migrations.start(P);
        awaitReady(g);
        assertThatThrownBy(() -> migrations.rollback(P)).isInstanceOf(ConflictException.class);
        migrations.cutover(P);
        migrations.rollback(P);
        assertThat(generations.live(P).orElseThrow().id()).isEqualTo(old.id());
        assertThat(status(g)).isEqualTo(GenerationStatus.READY);
        assertThat(status(old)).isEqualTo(GenerationStatus.LIVE);
    }

    @Test
    @Order(7)
    void finalizeDropsTable() {
        Generation ready = generations.targets(P).stream().filter(x -> x.status() == GenerationStatus.READY).findFirst().orElseThrow();
        Generation old = generations.live(P).orElseThrow();
        migrations.cutover(P);
        migrations.finalizeMigration(P);
        assertThat(status(old)).isEqualTo(GenerationStatus.DROPPED);
        assertThat(generations.live(P).orElseThrow().id()).isEqualTo(ready.id());
        tableExists(old, false);
        tableExists(ready, true);
        assertThatThrownBy(() -> migrations.finalizeMigration(P)).isInstanceOf(ConflictException.class);
    }
}
