package dev.driftless.server.ops;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import dev.driftless.server.support.IntegrationTest;
import dev.driftless.server.support.SourceDb;
import java.time.Duration;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.springframework.test.context.TestPropertySource;

@TestPropertySource(properties = {
        "driftless.pipelines[0].name=mt_articles",
        "driftless.pipelines[0].table=public.mt_articles",
        "driftless.pipelines[0].key=id",
        "driftless.pipelines[0].text={title}",
        "driftless.pipelines[0].embedding.provider=fake",
        "driftless.pipelines[0].embedding.model=fake-8",
        "driftless.pipelines[0].embedding.dimensions=8",
})
class MetricsIT extends IntegrationTest {

    @BeforeAll
    static void table() {
        SourceDb.createArticles(sourceJdbc(), "mt_articles");
    }

    private String scrape() {
        return management().get().uri("/actuator/prometheus").retrieve().body(String.class);
    }

    @Test
    void changesAndFreshnessAreExported() {
        awaitSlotActive("mt_articles");
        for (int i = 1; i <= 20; i++) {
            sourceJdbc().update("INSERT INTO mt_articles (id, title) VALUES (?, ?)", i, "Metric row " + i);
        }
        await().atMost(Duration.ofMinutes(1)).untilAsserted(() -> assertThat(scrape().lines().filter(l -> l.startsWith("driftless_changes")).toList())
                .contains("driftless_changes_total{application=\"driftless\",outcome=\"EMBED\",pipeline=\"mt_articles\"} 20.0"));
        String metrics = scrape();
        assertThat(metrics).containsPattern("driftless_freshness_seconds_bucket\\{application=\"driftless\",pipeline=\"mt_articles\",le=\"[0-9.]+\"} [1-9]")
                .contains("driftless_embed_chunks_total{application=\"driftless\",pipeline=\"mt_articles\",result=\"miss\"} 20.0")
                .contains("driftless_embed_provider_calls_total{application=\"driftless\",pipeline=\"mt_articles\"}")
                .contains("driftless_embed_estimated_cost_usd_total{application=\"driftless\",pipeline=\"mt_articles\"}")
                .contains("driftless_provider_circuit_open{application=\"driftless\",pipeline=\"mt_articles\"} 0.0")
                .contains("driftless_consumer_lag_records{application=\"driftless\",pipeline=\"mt_articles\"}")
                .contains("driftless_migration_progress_ratio{application=\"driftless\",pipeline=\"mt_articles\"}");
    }
}
