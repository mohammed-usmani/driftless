package dev.driftless.server.embed;

import static org.assertj.core.api.Assertions.assertThat;

import dev.driftless.server.support.IntegrationTest;
import java.sql.Timestamp;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;

class EmbeddingCacheIT extends IntegrationTest {

    @Autowired EmbeddingCache cache;

    @Test
    void roundTripsVectorsExactlyAndTouchesOnHit() {
        float[] v = {0.1f, -0.25f, 1e-7f, 3.4028235e38f};
        cache.putAll("m-rt", Map.of("h1", v));
        jdbc.update("UPDATE embedding_cache SET last_used_at = now() - interval '10 days' WHERE model = 'm-rt'");
        Map<String, float[]> got = cache.getAll("m-rt", List.of("h1", "missing"));
        assertThat(got).containsOnlyKeys("h1");
        assertThat(got.get("h1")).containsExactly(v);
        Timestamp touched = jdbc.queryForObject("SELECT last_used_at FROM embedding_cache WHERE model = 'm-rt'", Timestamp.class);
        assertThat(touched.toInstant()).isAfter(java.time.Instant.now().minusSeconds(60));
    }
}
