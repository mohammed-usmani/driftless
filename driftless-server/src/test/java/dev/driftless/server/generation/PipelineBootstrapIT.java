package dev.driftless.server.generation;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import dev.driftless.server.config.DriftlessProperties;
import dev.driftless.server.source.ConnectorManager;
import dev.driftless.server.support.IntegrationTest;
import dev.driftless.server.support.SourceDb;
import java.time.Duration;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.TestPropertySource;

@TestPropertySource(properties = {
        "driftless.pipelines[0].name=bt_ok",
        "driftless.pipelines[0].table=public.bt_ok",
        "driftless.pipelines[0].key=id",
        "driftless.pipelines[0].text={title}",
        "driftless.pipelines[0].metadata=category",
        "driftless.pipelines[0].embedding.provider=fake",
        "driftless.pipelines[0].embedding.model=fake-8",
        "driftless.pipelines[0].embedding.dimensions=8",
        "driftless.pipelines[1].name=bt_blocked",
        "driftless.pipelines[1].table=public.bt_blocked",
        "driftless.pipelines[1].key=id",
        "driftless.pipelines[1].text={title}",
        "driftless.pipelines[1].embedding.provider=fake",
        "driftless.pipelines[1].embedding.model=fake-8",
        "driftless.pipelines[1].embedding.dimensions=8",
})
class PipelineBootstrapIT extends IntegrationTest {

    @Autowired GenerationRepository generations;
    @Autowired ConnectorManager connectors;
    @Autowired PipelineBootstrap bootstrap;
    @Autowired DriftlessProperties props;

    @BeforeAll
    static void tables() {
        SourceDb.createArticles(sourceJdbc(), "bt_ok");
        SourceDb.createArticles(sourceJdbc(), "bt_blocked");
        sourceJdbc().execute("ALTER TABLE bt_blocked REPLICA IDENTITY DEFAULT");
    }

    @Test
    void readyPipelineGetsALiveGenerationAndAConnector() {
        assertThat(generations.status("bt_ok")).isEqualTo(PipelineStatus.ACTIVE);
        Generation live = generations.live("bt_ok").orElseThrow();
        assertThat(live.number()).isEqualTo(1);
        assertThat(live.status()).isEqualTo(GenerationStatus.LIVE);
        assertThat(jdbc.queryForObject("SELECT to_regclass(?) IS NOT NULL", Boolean.class, live.chunkTable())).isTrue();
        await().atMost(Duration.ofMinutes(1)).until(() -> connectors.state("bt_ok") == ConnectorManager.State.RUNNING);
    }

    @Test
    void bootingAgainKeepsTheSameGeneration() {
        bootstrap.run(props.pipeline("bt_ok"));
        assertThat(generations.targets("bt_ok")).hasSize(1);
        assertThat(generations.live("bt_ok").orElseThrow().number()).isEqualTo(1);
    }

    @Test
    void unsafeTableIsBlockedWithoutAffectingOthers() {
        assertThat(generations.status("bt_blocked")).isEqualTo(PipelineStatus.BLOCKED);
        assertThat(generations.blockedReason("bt_blocked")).contains("REPLICA IDENTITY FULL");
        assertThat(generations.live("bt_blocked")).isEmpty();
        assertThat(connectors.state("bt_blocked")).isEqualTo(ConnectorManager.State.MISSING);
        assertThat(generations.status("bt_ok")).isEqualTo(PipelineStatus.ACTIVE);
    }
}
