package dev.driftless.server.search;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import dev.driftless.server.embed.EmbeddingProvider;
import dev.driftless.server.embed.ProviderUnavailableException;
import dev.driftless.server.support.ControllableFake;
import dev.driftless.server.support.IntegrationTest;
import dev.driftless.server.support.SourceDb;
import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Import;
import org.springframework.core.ParameterizedTypeReference;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.test.context.TestPropertySource;
import org.springframework.transaction.support.TransactionSynchronizationManager;

@TestPropertySource(properties = {
        "driftless.pipelines[0].name=sr_articles",
        "driftless.pipelines[0].table=public.sr_articles",
        "driftless.pipelines[0].key=id",
        "driftless.pipelines[0].text={title}\\n\\n{body}",
        "driftless.pipelines[0].metadata=category,tenant_id,acl",
        "driftless.pipelines[0].embedding.provider=fake",
        "driftless.pipelines[0].embedding.model=fake-8",
        "driftless.pipelines[0].embedding.dimensions=8",
})
@Import(ControllableFake.Config.class)
class SearchIT extends IntegrationTest {

    @Autowired SearchService service;
    @Autowired @Qualifier("fake") EmbeddingProvider provider;

    private static final String[] CATEGORIES = {"billing", "account", "other"};

    @BeforeAll
    static void seed() {
        SourceDb.createArticles(sourceJdbc(), "sr_articles");
        List<Object[]> rows = new ArrayList<>();
        for (int i = 1; i <= 2000; i++) {
            rows.add(new Object[] {i, "Article " + i, "Body " + i, CATEGORIES[i % 3], i % 100 == 0 ? "t-rare" : "t-common",
                    i % 2 == 0 ? "{u1}" : "{u2}"});
        }
        rows.add(new Object[] {5000, "Reset a password", "Open settings.", "account", "t-common", "{u1}"});
        sourceJdbc().batchUpdate("INSERT INTO sr_articles (id, title, body, category, tenant_id, acl) VALUES (?, ?, ?, ?, ?, ?::text[])", rows);
    }

    @BeforeEach
    void backfilled() {
        await().atMost(Duration.ofMinutes(3)).until(() -> jdbc.queryForObject("""
                SELECT count(*) FROM row_state r JOIN pipeline p ON p.live_generation_id = r.generation_id
                WHERE p.name = 'sr_articles' AND NOT r.deleted""", Long.class) == 2001);
    }

    private ResponseEntity<Map<String, Object>> search(String pipeline, Map<String, Object> body) {
        return client().post().uri("/v1/pipelines/{p}/search", pipeline)
                .header("Authorization", "Bearer " + ADMIN_TOKEN)
                .contentType(MediaType.APPLICATION_JSON).body(body)
                .retrieve().toEntity(new ParameterizedTypeReference<>() { });
    }

    @SuppressWarnings("unchecked")
    private static List<Map<String, Object>> results(ResponseEntity<Map<String, Object>> r) {
        assertThat(r.getStatusCode().value()).as("%s", r.getBody()).isEqualTo(200);
        return (List<Map<String, Object>>) r.getBody().get("results");
    }

    @SuppressWarnings("unchecked")
    private static Map<String, Object> meta(Map<String, Object> result) {
        return (Map<String, Object>) result.get("metadata");
    }

    @Test
    void returnsNearestChunksWithScores() {
        var r = search("sr_articles", Map.of("query", "Reset a password\n\nOpen settings.", "k", 5));
        List<Map<String, Object>> results = results(r);
        assertThat(r.getBody().get("generation")).isEqualTo(1);
        assertThat(results).hasSize(5);
        assertThat(results.getFirst()).containsEntry("key", "5000").containsEntry("chunkNo", 0)
                .containsEntry("text", "Reset a password\n\nOpen settings.");
        assertThat(((Number) results.getFirst().get("score")).doubleValue()).isGreaterThan(0.999);
        assertThat(results).extracting(x -> ((Number) x.get("score")).doubleValue()).isSortedAccordingTo((a, b) -> Double.compare(b, a));
    }

    @Test
    void filterByCategory() {
        List<Map<String, Object>> results = results(search("sr_articles", Map.of("query", "anything", "k", 20,
                "filter", Map.of("category", List.of("billing", "account")))));
        assertThat(results).hasSize(20).allSatisfy(x -> assertThat(meta(x).get("category")).isIn("billing", "account"));
    }

    @Test
    @SuppressWarnings("unchecked")
    void principalOnlySeesAclRows() {
        List<Map<String, Object>> results = results(search("sr_articles", Map.of("query", "anything", "k", 30, "principal", "u2")));
        assertThat(results).hasSize(30).allSatisfy(x -> assertThat((List<String>) meta(x).get("acl")).contains("u2"));
    }

    @Test
    void selectiveFilterStillReturnsK() {
        List<Map<String, Object>> results = results(search("sr_articles", Map.of("query", "Article 7", "k", 10,
                "filter", Map.of("tenant_id", "t-rare"))));
        assertThat(results).hasSize(10).allSatisfy(x -> assertThat(meta(x).get("tenant_id")).isEqualTo("t-rare"));
    }

    @Test
    void unknownFilterColumnIs400() {
        var r = search("sr_articles", Map.of("query", "q", "filter", Map.of("title", "x")));
        assertThat(r.getStatusCode().value()).isEqualTo(400);
        assertThat(r.getHeaders().getContentType().toString()).startsWith("application/problem+json");
        assertThat((String) r.getBody().get("detail")).contains("title");
    }

    @Test
    void kAbove100Is400() {
        var r = search("sr_articles", Map.of("query", "q", "k", 101));
        assertThat(r.getStatusCode().value()).isEqualTo(400);
        assertThat(r.getBody()).containsKey("errors");
        assertThat(r.getBody().get("errors").toString()).contains("k");
    }

    @Test
    void queryIsEmbeddedWithoutHoldingADatabaseConnection() {
        ControllableFake fake = (ControllableFake) provider;
        AtomicBoolean inTransaction = new AtomicBoolean();
        fake.onEmbed = () -> inTransaction.set(TransactionSynchronizationManager.isActualTransactionActive());
        try {
            assertThat(service.search("sr_articles", "Reset a password", 3, null, null).results()).hasSize(3);
        } finally {
            fake.onEmbed = () -> { };
        }
        assertThat(inTransaction).as("a transaction (and its connection) was open during the provider call").isFalse();
    }

    @Test
    void providerOutageFailsASearchAfterOneAttempt() {
        ControllableFake fake = (ControllableFake) provider;
        int before = fake.calls.get();
        fake.downUntil = Instant.now().plusSeconds(60);
        try {
            org.assertj.core.api.Assertions.assertThatThrownBy(() -> service.search("sr_articles", "q", 3, null, null))
                    .isInstanceOf(ProviderUnavailableException.class);
        } finally {
            fake.downUntil = Instant.MIN;
        }
        assertThat(fake.calls.get() - before).as("provider attempts for one search").isEqualTo(1);
    }

    @Test
    void unknownPipelineIs404() {
        var r = search("nope", Map.of("query", "q"));
        assertThat(r.getStatusCode().value()).isEqualTo(404);
        assertThat(r.getBody()).containsEntry("status", 404);
    }
}
