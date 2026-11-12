package dev.driftless.server.embed;

import dev.driftless.server.change.Hashes;
import java.util.ArrayList;
import java.util.List;
import java.util.SplittableRandom;

/**
 * Deterministic unit vectors seeded from the text. Used by tests and by the load test, to measure the
 * pipeline separately from a model. Similar texts don't get similar vectors; search results are meaningless.
 */
public class FakeProvider implements EmbeddingProvider {

    private final int latencyMs;

    public FakeProvider(int latencyMs) {
        this.latencyMs = latencyMs;
    }

    @Override
    public List<float[]> embed(EmbeddingSpec spec, List<String> texts) {
        if (latencyMs > 0) {
            try {
                Thread.sleep(latencyMs);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new ProviderUnavailableException("interrupted");
            }
        }
        List<float[]> out = new ArrayList<>(texts.size());
        for (String t : texts) {
            SplittableRandom rnd = new SplittableRandom(Long.parseUnsignedLong(Hashes.sha256Hex(spec.model() + "\u0000" + t).substring(0, 15), 16));
            float[] v = new float[spec.dimensions()];
            double norm = 0;
            for (int i = 0; i < v.length; i++) {
                v[i] = (float) (rnd.nextDouble() * 2 - 1);
                norm += v[i] * v[i];
            }
            norm = Math.sqrt(norm);
            for (int i = 0; i < v.length; i++) {
                v[i] = (float) (v[i] / norm);
            }
            out.add(v);
        }
        return out;
    }
}
