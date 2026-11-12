package dev.driftless.server.embed;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

class EmbeddingServiceTest {

    private final EmbeddingSpec spec = new EmbeddingSpec("counting", "m", 4);

    /** Fake provider that counts calls and can fail the first N of them. */
    static final class Counting implements EmbeddingProvider {
        final List<Integer> batchSizes = new ArrayList<>();
        int failFirst;
        int dims = 4;

        @Override
        public List<float[]> embed(EmbeddingSpec spec, List<String> texts) {
            batchSizes.add(texts.size());
            if (failFirst-- > 0) {
                throw new ProviderUnavailableException("down");
            }
            return texts.stream().map(t -> new float[dims]).toList();
        }
    }

    /** In-memory cache. */
    static final class MapCache extends EmbeddingCache {
        final Map<String, float[]> map = new HashMap<>();

        MapCache() {
            super(null);
        }

        @Override
        public Map<String, float[]> getAll(String model, java.util.Collection<String> hashes) {
            Map<String, float[]> out = new HashMap<>();
            hashes.forEach(h -> { if (map.containsKey(model + h)) out.put(h, map.get(model + h)); });
            return out;
        }

        @Override
        public void putAll(String model, Map<String, float[]> vectors) {
            vectors.forEach((h, v) -> map.put(model + h, v));
        }
    }

    static final class TestClock extends Clock {
        Instant now = Instant.parse("2026-01-01T00:00:00Z");
        public java.time.ZoneId getZone() { return ZoneOffset.UTC; }
        public Clock withZone(java.time.ZoneId z) { return this; }
        public Instant instant() { return now; }
    }

    private final Counting provider = new Counting();
    private final MapCache cache = new MapCache();
    private final TestClock clock = new TestClock();
    private final List<Long> sleeps = new ArrayList<>();
    private final EmbeddingService service = new EmbeddingService(Map.of("counting", provider), cache, 64, clock, sleeps::add);

    @Test
    void cacheHitSkipsProvider() {
        service.embed(spec, List.of("a", "b"));
        EmbedResult second = service.embed(spec, List.of("a", "b"));
        assertThat(provider.batchSizes).hasSize(1);
        assertThat(second.cacheHits()).isEqualTo(2);
        assertThat(second.providerCalls()).isZero();
        assertThat(second.vectors()).hasSize(2);
    }

    @Test
    void duplicateChunksEmbeddedOnce() {
        EmbedResult r = service.embed(spec, List.of("a", "a", "b"));
        assertThat(provider.batchSizes).containsExactly(2);
        assertThat(r.embedded()).isEqualTo(2);
        assertThat(r.vectors()).hasSize(3);
    }

    @Test
    void splitsLargeRequestsIntoProviderBatches() {
        List<String> chunks = new ArrayList<>();
        for (int i = 0; i < 300; i++) {
            chunks.add("chunk " + i);
        }
        EmbedResult r = service.embed(spec, chunks);
        assertThat(provider.batchSizes).containsExactly(64, 64, 64, 64, 44);
        assertThat(r.providerCalls()).isEqualTo(5);
    }

    @Test
    void retriesThenSucceeds() {
        provider.failFirst = 2;
        EmbedResult r = service.embed(spec, List.of("a"));
        assertThat(r.vectors()).hasSize(1);
        assertThat(sleeps).containsExactly(200L, 400L);
    }

    @Test
    void breakerOpensAfterFiveFailures() {
        provider.failFirst = 1000;
        assertThatThrownBy(() -> service.embed(spec, List.of("a"))).isInstanceOf(ProviderUnavailableException.class);
        assertThat(provider.batchSizes).hasSize(5);
        assertThatThrownBy(() -> service.embed(spec, List.of("a")))
                .isInstanceOf(ProviderUnavailableException.class).hasMessageContaining("circuit open");
        assertThat(provider.batchSizes).hasSize(5);
        assertThat(service.circuitOpen("counting")).isTrue();

        clock.now = clock.now.plus(Duration.ofSeconds(11));
        provider.failFirst = 0;
        assertThat(service.embed(spec, List.of("a")).vectors()).hasSize(1);
        assertThat(service.circuitOpen("counting")).isFalse();
    }

    @Test
    void wrongDimensionsRejected() {
        provider.dims = 3;
        assertThatThrownBy(() -> service.embed(spec, List.of("a")))
                .isInstanceOf(IllegalStateException.class).hasMessageContaining("3").hasMessageContaining("4");
    }

    @Test
    void noChunksNoCalls() {
        EmbedResult r = service.embed(spec, List.of());
        assertThat(r.vectors()).isEmpty();
        assertThat(provider.batchSizes).isEmpty();
    }
}
