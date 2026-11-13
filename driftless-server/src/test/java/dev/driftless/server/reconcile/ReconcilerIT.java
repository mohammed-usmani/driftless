package dev.driftless.server.reconcile;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import dev.driftless.server.generation.GenerationRepository;
import dev.driftless.server.support.IntegrationTest;
import dev.driftless.server.support.RandomWriters;
import dev.driftless.server.support.SourceDb;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.MediaType;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.TestPropertySource;

@ActiveProfiles("dev")
@TestPropertySource(properties = {
        "driftless.reconciler.sample-size=1000",
        "driftless.reconciler.grace-seconds=3",
        "driftless.pipelines[0].name=rc_articles",
        "driftless.pipelines[0].table=public.rc_articles",
        "driftless.pipelines[0].key=id",
        "driftless.pipelines[0].text={title}\\n\\n{body}",
        "driftless.pipelines[0].metadata=category,tenant_id,acl",
        "driftless.pipelines[0].embedding.provider=fake",
        "driftless.pipelines[0].embedding.model=fake-8",
        "driftless.pipelines[0].embedding.dimensions=8",
        "driftless.pipelines[1].name=rc_uuid",
        "driftless.pipelines[1].table=public.rc_uuid",
        "driftless.pipelines[1].key=id",
        "driftless.pipelines[1].text={title}\\n\\n{body}",
        "driftless.pipelines[1].metadata=category",
        "driftless.pipelines[1].embedding.provider=fake",
        "driftless.pipelines[1].embedding.model=fake-8",
        "driftless.pipelines[1].embedding.dimensions=8",
        "driftless.pipelines[2].name=rc_priced",
        "driftless.pipelines[2].table=public.rc_priced",
        "driftless.pipelines[2].key=id",
        "driftless.pipelines[2].text={title}",
        "driftless.pipelines[2].metadata=acl,price",
        "driftless.pipelines[2].embedding.provider=fake",
        "driftless.pipelines[2].embedding.model=fake-8",
        "driftless.pipelines[2].embedding.dimensions=8",
        "driftless.pipelines[3].name=rc_decimal",
        "driftless.pipelines[3].table=public.rc_decimal",
        "driftless.pipelines[3].key=id",
        "driftless.pipelines[3].text={title}",
        "driftless.pipelines[3].embedding.provider=fake",
        "driftless.pipelines[3].embedding.model=fake-8",
        "driftless.pipelines[3].embedding.dimensions=8",
        "driftless.pipelines[4].name=rc_guid",
        "driftless.pipelines[4].table=public.rc_guid",
        "driftless.pipelines[4].key=id",
        "driftless.pipelines[4].text={title}",
        "driftless.pipelines[4].embedding.provider=fake",
        "driftless.pipelines[4].embedding.model=fake-8",
        "driftless.pipelines[4].embedding.dimensions=8",
})
class ReconcilerIT extends IntegrationTest {

    @Autowired Reconciler reconciler;
    @Autowired GenerationRepository generations;

    @BeforeAll
    static void seed() {
        SourceDb.createArticles(sourceJdbc(), "rc_articles");
        SourceDb.create(sourceJdbc(), "rc_uuid", "uuid", "");
        SourceDb.create(sourceJdbc(), "rc_priced", "bigint", ", price numeric(10,2)");
        SourceDb.create(sourceJdbc(), "rc_decimal", "numeric(10,2)", "");
        SourceDb.create(sourceJdbc(), "rc_guid", "text", "");
        List<Object[]> decimals = new ArrayList<>();
        List<Object[]> guids = new ArrayList<>();
        for (int i = 0; i < 50; i++) {
            decimals.add(new Object[] {new java.math.BigDecimal(i + ".50"), "Decimal " + i});
            guids.add(new Object[] {UUID.randomUUID().toString().toUpperCase(java.util.Locale.ROOT), "Guid " + i});
        }
        sourceJdbc().batchUpdate("INSERT INTO rc_decimal (id, title) VALUES (?, ?)", decimals);
        sourceJdbc().batchUpdate("INSERT INTO rc_guid (id, title) VALUES (?, ?)", guids);
        List<Object[]> rows = new ArrayList<>();
        List<Object[]> uuids = new ArrayList<>();
        List<Object[]> priced = new ArrayList<>();
        for (int i = 1001; i <= 1300; i++) {
            rows.add(new Object[] {i, "Seeded " + i, "body " + i, "cat-" + i % 4, "t-" + i % 3, i % 2 == 0 ? "{u1}" : "{u1,admins}"});
        }
        for (int i = 0; i < 100; i++) {
            uuids.add(new Object[] {UUID.randomUUID(), "Uuid row " + i, "body", "c"});
            priced.add(new Object[] {i, "Priced " + i, i % 3 == 0 ? "{}" : "{u1,u2}", new java.math.BigDecimal(i + ".50")});
        }
        sourceJdbc().batchUpdate("INSERT INTO rc_articles (id, title, body, category, tenant_id, acl) VALUES (?, ?, ?, ?, ?, ?::text[])", rows);
        sourceJdbc().batchUpdate("INSERT INTO rc_uuid (id, title, body, category) VALUES (?, ?, ?, ?)", uuids);
        sourceJdbc().batchUpdate("INSERT INTO rc_priced (id, title, acl, price) VALUES (?, ?, ?::text[], ?)", priced);
    }

    private long live(String pipeline) {
        return generations.live(pipeline).map(g -> jdbc.queryForObject(
                "SELECT count(*) FROM row_state WHERE generation_id = ? AND NOT deleted", Long.class, g.id())).orElse(0L);
    }

    @BeforeEach
    void backfilled() {
        await().atMost(Duration.ofMinutes(3)).until(() -> live("rc_uuid") == 100 && live("rc_priced") == 100 && live("rc_articles") >= 300
                && live("rc_decimal") == 50 && live("rc_guid") == 50);
    }

    private void chaos(String pipeline, String kind, int count) {
        var r = client().post().uri("/v1/chaos/pipelines/{p}/{k}", pipeline, kind)
                .header("Authorization", "Bearer " + ADMIN_TOKEN).contentType(MediaType.APPLICATION_JSON)
                .body(Map.of("count", count)).retrieve().toBodilessEntity();
        assertThat(r.getStatusCode().value()).isEqualTo(200);
    }

    private static int drift(DriftReport r) {
        return r.missing() + r.mismatched() + r.leaked();
    }

    @Test
    void cleanIndexReportsNoDrift() {
        DriftReport r = reconciler.run("rc_articles");
        assertThat(r.sampledSource()).isPositive();
        assertThat(r.sampledIndex()).isPositive();
        assertThat(drift(r)).isZero();
    }

    @Test
    void corruptedRowsDetectedAndRepaired() {
        chaos("rc_articles", "corrupt", 5);
        DriftReport r = reconciler.run("rc_articles");
        assertThat(r.mismatched()).isEqualTo(5);
        reconciler.repair(r.id());
        await().atMost(Duration.ofMinutes(2)).until(() -> drift(reconciler.run("rc_articles")) == 0);
    }

    @Test
    void injectedLeaksDetectedAndDeleted() {
        chaos("rc_articles", "leak", 3);
        DriftReport r = reconciler.run("rc_articles");
        assertThat(r.leaked()).isEqualTo(3);
        List<String> keys = jdbc.queryForList("SELECT row_key FROM drift_item WHERE report_id = ? AND kind = 'LEAK'", String.class, r.id());
        reconciler.repair(r.id());
        String table = generations.live("rc_articles").orElseThrow().chunkTable();
        await().atMost(Duration.ofMinutes(1)).until(() -> jdbc.queryForObject(
                "SELECT count(*) FROM " + table + " WHERE row_key = ANY (?)", Long.class, (Object) keys.toArray(String[]::new)) == 0);
        assertThat(jdbc.queryForObject("SELECT repaired FROM drift_report WHERE id = ?", Integer.class, r.id())).isEqualTo(3);
        assertThat(drift(reconciler.run("rc_articles"))).isZero();
    }

    @Test
    void recentChangesInsideGraceWindowNotReported() throws Exception {
        Thread writers = Thread.ofPlatform().start(() -> RandomWriters.run(sourceJdbcUrl(), POSTGRES.getUsername(),
                POSTGRES.getPassword(), "rc_articles", 3, Duration.ofSeconds(40), 50, 7));
        Thread.sleep(2_000);
        List<String> reports = new ArrayList<>();
        for (int i = 0; i < 5 && writers.isAlive(); i++) {
            DriftReport r = reconciler.run("rc_articles");
            if (drift(r) > 0) {
                reports.add(r + " " + jdbc.queryForList("SELECT row_key || ' ' || kind || ' ' || coalesce(detail, '') FROM drift_item WHERE report_id = ?",
                        String.class, r.id()));
            }
        }
        writers.join();
        assertThat(reports).isEmpty();
    }

    @Test
    void uuidKeyTableHasNoFalseDrift() {
        DriftReport r = reconciler.run("rc_uuid");
        assertThat(r.sampledSource()).isEqualTo(100);
        assertThat(drift(r)).isZero();
    }

    @Test
    void numericKeyWithScaleHasNoFalseDrift() {
        DriftReport r = reconciler.run("rc_decimal");
        assertThat(r.sampledSource()).isEqualTo(50);
        assertThat(drift(r)).isZero();
    }

    @Test
    void uppercaseTextKeyShapedLikeAUuidHasNoFalseDrift() {
        DriftReport r = reconciler.run("rc_guid");
        assertThat(r.sampledSource()).isEqualTo(50);
        assertThat(drift(r)).isZero();
    }

    @Test
    void arrayAndNumericMetadataHaveNoFalseDrift() {
        DriftReport r = reconciler.run("rc_priced");
        assertThat(r.sampledSource()).isEqualTo(100);
        assertThat(drift(r)).isZero();
    }
}
