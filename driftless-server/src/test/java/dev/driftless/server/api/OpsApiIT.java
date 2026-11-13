package dev.driftless.server.api;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import dev.driftless.server.support.IntegrationTest;
import dev.driftless.server.support.SourceDb;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.core.ParameterizedTypeReference;
import org.springframework.http.ResponseEntity;
import org.springframework.test.context.TestPropertySource;

@TestPropertySource(properties = {
        "driftless.reconciler.grace-seconds=1",
        "driftless.pipelines[0].name=op_articles",
        "driftless.pipelines[0].table=public.op_articles",
        "driftless.pipelines[0].key=id",
        "driftless.pipelines[0].text={title}\\n\\n{body}",
        "driftless.pipelines[0].metadata=category,acl",
        "driftless.pipelines[0].embedding.provider=fake",
        "driftless.pipelines[0].embedding.model=fake-8",
        "driftless.pipelines[0].embedding.dimensions=8",
})
class OpsApiIT extends IntegrationTest {

    private static final String P = "op_articles";

    @BeforeAll
    static void seed() {
        SourceDb.createArticles(sourceJdbc(), P);
        for (int i = 1; i <= 12; i++) {
            sourceJdbc().update("INSERT INTO op_articles (id, title, body, category, acl) VALUES (?, ?, 'body', 'c', '{u1}')", i, "Row " + i);
        }
    }

    @BeforeEach
    void backfilled() {
        await().atMost(Duration.ofMinutes(2)).until(() -> jdbc.queryForObject("""
                SELECT count(*) FROM row_state r JOIN pipeline p ON p.live_generation_id = r.generation_id
                WHERE p.name = ? AND NOT r.deleted""", Long.class, P) == 12);
    }

    private ResponseEntity<Object> call(String method, String uri) {
        return client().method(org.springframework.http.HttpMethod.valueOf(method)).uri(uri)
                .header("Authorization", "Bearer " + ADMIN_TOKEN).retrieve().toEntity(new ParameterizedTypeReference<>() { });
    }

    @SuppressWarnings("unchecked")
    private Map<String, Object> object(String method, String uri, int status) {
        ResponseEntity<Object> r = call(method, uri);
        assertThat(r.getStatusCode().value()).as("%s %s → %s", method, uri, r.getBody()).isEqualTo(status);
        return (Map<String, Object>) r.getBody();
    }

    @SuppressWarnings("unchecked")
    private List<Map<String, Object>> list(String uri) {
        ResponseEntity<Object> r = call("GET", uri);
        assertThat(r.getStatusCode().value()).as("%s", r.getBody()).isEqualTo(200);
        return (List<Map<String, Object>>) r.getBody();
    }

    @SuppressWarnings("unchecked")
    private List<Map<String, Object>> data(Map<String, Object> page) {
        return (List<Map<String, Object>>) page.get("data");
    }

    @Test
    void listsPipelines() {
        Map<String, Object> p = list("/v1/pipelines").stream().filter(x -> P.equals(x.get("name"))).findFirst().orElseThrow();
        assertThat(p).containsEntry("table", "public.op_articles").containsEntry("status", "ACTIVE").containsEntry("liveGeneration", 1)
                .containsEntry("rows", 12).containsEntry("chunks", 12).containsEntry("failedOpen", 0).containsEntry("textStale", 0)
                .containsKeys("lagSeconds", "embedCallsSaved", "blockedReason");
    }

    @Test
    @SuppressWarnings("unchecked")
    void pipelineDetail() {
        Map<String, Object> p = object("GET", "/v1/pipelines/" + P, 200);
        assertThat((List<Object>) p.get("generations")).isNotEmpty();
        assertThat((Map<String, Object>) p.get("freshness")).containsKeys("p50Ms", "p99Ms");
        assertThat(((Number) ((Map<String, Object>) p.get("outcomes")).get("EMBED")).intValue()).isGreaterThanOrEqualTo(12);
        assertThat(p).containsKey("migration");
        assertThat(p).as("the connector's state, so a failed Debezium task is visible").containsEntry("connector", "RUNNING");
        assertThat(object("GET", "/v1/pipelines/nope", 404)).containsEntry("status", 404);
    }

    @Test
    void changesPageDisjointlyAndCoverEverything() {
        long total = jdbc.queryForObject("SELECT count(*) FROM recent_change WHERE pipeline = ?", Long.class, P);
        Set<Object> seen = new HashSet<>();
        List<Object> ordered = new ArrayList<>();
        String cursor = null;
        do {
            Map<String, Object> page = object("GET", "/v1/pipelines/" + P + "/changes?limit=5" + (cursor == null ? "" : "&cursor=" + cursor), 200);
            for (Map<String, Object> c : data(page)) {
                assertThat(seen.add(c.get("id"))).as("duplicate %s", c.get("id")).isTrue();
                ordered.add(c.get("id"));
                assertThat(c).containsKeys("key", "outcome", "freshnessMs", "appliedAt");
            }
            cursor = (String) page.get("nextCursor");
        } while (cursor != null);
        assertThat(seen.size()).isGreaterThanOrEqualTo((int) total);
        assertThat(ordered).isSortedAccordingTo((a, b) -> Long.compare(((Number) b).longValue(), ((Number) a).longValue()));
        assertThat(call("GET", "/v1/pipelines/" + P + "/changes?limit=0").getStatusCode().value()).isEqualTo(400);
    }

    @Test
    void failedChangesListAndRetry() {
        long id = jdbc.queryForObject("INSERT INTO failed_change (pipeline, row_key, reason) VALUES (?, '5', 'test failure') RETURNING id", Long.class, P);
        List<Map<String, Object>> open = data(object("GET", "/v1/pipelines/" + P + "/failed-changes?status=OPEN", 200));
        assertThat(open).anySatisfy(f -> assertThat(f).containsEntry("rowKey", "5").containsEntry("reason", "test failure"));
        object("POST", "/v1/failed-changes/" + id + "/retry", 202);
        assertThat(jdbc.queryForObject("SELECT status FROM failed_change WHERE id = ?", String.class, id)).isIn("RETRYING", "RESOLVED");
        assertThat(object("POST", "/v1/failed-changes/999999/retry", 404)).containsEntry("status", 404);
    }

    @Test
    void migrationConflictsAreProblems() {
        Map<String, Object> unchanged = object("POST", "/v1/pipelines/" + P + "/migrations", 409);
        assertThat(unchanged).containsEntry("status", 409).containsKey("detail");
        assertThat(call("POST", "/v1/pipelines/" + P + "/migrations/cutover").getHeaders().getContentType().toString())
                .startsWith("application/problem+json");
        object("POST", "/v1/pipelines/" + P + "/migrations/rollback", 409);
        object("POST", "/v1/pipelines/" + P + "/migrations/finalize", 409);
    }

    @Test
    @SuppressWarnings("unchecked")
    void reconcileAndDriftReports() {
        int before = data(object("GET", "/v1/pipelines/" + P + "/drift-reports", 200)).size();
        object("POST", "/v1/pipelines/" + P + "/reconcile", 202);
        await().atMost(Duration.ofSeconds(30)).until(() -> data(object("GET", "/v1/pipelines/" + P + "/drift-reports", 200)).size() > before);
        Map<String, Object> latest = data(object("GET", "/v1/pipelines/" + P + "/drift-reports?limit=1", 200)).getFirst();
        Map<String, Object> report = object("GET", "/v1/drift-reports/" + latest.get("id"), 200);
        assertThat(report).containsKeys("missing", "mismatched", "leaked", "items");
        assertThat((List<Object>) report.get("items")).isEmpty();
        object("POST", "/v1/drift-reports/" + latest.get("id") + "/repair", 202);
        object("GET", "/v1/drift-reports/999999", 404);
    }

    @Test
    void snapshotQueue() {
        assertThat(list("/v1/pipelines/" + P + "/snapshots")).anySatisfy(s ->
                assertThat(s).containsEntry("reason", "BACKFILL").containsEntry("kind", "FULL").containsKey("status"));
    }
}
