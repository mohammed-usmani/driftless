package dev.driftless.server.embed;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import org.junit.jupiter.api.Test;

class FakeProviderTest {

    private final FakeProvider fake = new FakeProvider(0);
    private final EmbeddingSpec spec = new EmbeddingSpec("fake", "fake-16", 16);

    @Test
    void sameTextGivesTheSameVector() {
        assertThat(fake.embed(spec, List.of("hello")).getFirst()).isEqualTo(fake.embed(spec, List.of("hello")).getFirst());
        assertThat(fake.embed(spec, List.of("hello")).getFirst()).isNotEqualTo(fake.embed(spec, List.of("world")).getFirst());
    }

    @Test
    void honoursDimensionsAndIsUnitLength() {
        float[] v = fake.embed(spec, List.of("hello")).getFirst();
        assertThat(v).hasSize(16);
        double norm = 0;
        for (float x : v) {
            norm += x * x;
        }
        assertThat(Math.sqrt(norm)).isCloseTo(1.0, org.assertj.core.data.Offset.offset(1e-6));
    }
}
