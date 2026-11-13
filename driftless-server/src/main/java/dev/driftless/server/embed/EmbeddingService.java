package dev.driftless.server.embed;

import dev.driftless.server.change.Hashes;
import dev.driftless.server.config.DriftlessProperties;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.LongConsumer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

/**
 * Embeds chunks through the cache: only distinct, uncached chunks reach the provider, in batches, with
 * retries and a per-provider circuit breaker so an outage doesn't turn into a hammering loop.
 */
@Service
public class EmbeddingService {

    static final int ATTEMPTS = 5;
    static final long BACKOFF_BASE_MS = 200;
    static final long BACKOFF_CAP_MS = 5_000;
    static final int BREAKER_THRESHOLD = 5;
    static final Duration BREAKER_OPEN = Duration.ofSeconds(10);

    private final Map<String, EmbeddingProvider> providers;
    private final EmbeddingCache cache;
    private final int batchSize;
    private final Clock clock;
    private final LongConsumer sleeper;
    private final Map<String, Breaker> breakers = new ConcurrentHashMap<>();

    private static final class Breaker {
        int consecutiveFailures;
        Instant openUntil = Instant.MIN;
    }

    @Autowired
    public EmbeddingService(Map<String, EmbeddingProvider> providers, EmbeddingCache cache,
                            DriftlessProperties props, Clock clock) {
        this(providers, cache, props.embedding().batchSize(), clock, EmbeddingService::sleep);
    }

    EmbeddingService(Map<String, EmbeddingProvider> providers, EmbeddingCache cache, int batchSize,
                     Clock clock, LongConsumer sleeper) {
        this.providers = providers;
        this.cache = cache;
        this.batchSize = batchSize;
        this.clock = clock;
        this.sleeper = sleeper;
    }

    public EmbedResult embed(EmbeddingSpec spec, List<String> chunks) {
        if (chunks.isEmpty()) {
            return new EmbedResult(List.of(), 0, 0, 0, new boolean[0]);
        }
        Map<String, String> textByHash = new LinkedHashMap<>();
        List<String> hashes = new ArrayList<>(chunks.size());
        for (String c : chunks) {
            String h = Hashes.sha256Hex(c);
            hashes.add(h);
            textByHash.putIfAbsent(h, c);
        }
        Map<String, float[]> vectors = new HashMap<>(cache.getAll(spec.model(), textByHash.keySet()));
        vectors.values().removeIf(v -> v.length != spec.dimensions());
        int hits = vectors.size();
        boolean[] cached = new boolean[hashes.size()];
        for (int i = 0; i < cached.length; i++) {
            cached[i] = vectors.containsKey(hashes.get(i));
        }

        List<String> missing = textByHash.keySet().stream().filter(h -> !vectors.containsKey(h)).toList();
        int calls = 0;
        for (int from = 0; from < missing.size(); from += batchSize) {
            List<String> batch = missing.subList(from, Math.min(missing.size(), from + batchSize));
            List<float[]> got = call(spec, batch.stream().map(textByHash::get).toList());
            calls++;
            Map<String, float[]> fresh = new HashMap<>();
            for (int i = 0; i < batch.size(); i++) {
                fresh.put(batch.get(i), got.get(i));
            }
            cache.putAll(spec.model(), fresh);
            vectors.putAll(fresh);
        }
        return new EmbedResult(hashes.stream().map(vectors::get).toList(), hits, missing.size(), calls, cached);
    }

    /** One query vector, not cached (queries rarely repeat and must not fill the cache). */
    /** One attempt: a person is waiting, and the caller reports an outage faster than a retry loop would. */
    public float[] embedQuery(EmbeddingSpec spec, String query) {
        return call(spec, List.of(query), 1).getFirst();
    }

    public boolean circuitOpen(String provider) {
        Breaker b = breakers.get(provider);
        return b != null && clock.instant().isBefore(b.openUntil);
    }

    private List<float[]> call(EmbeddingSpec spec, List<String> texts) {
        return call(spec, texts, ATTEMPTS);
    }

    private List<float[]> call(EmbeddingSpec spec, List<String> texts, int attempts) {
        EmbeddingProvider provider = providers.get(spec.provider());
        if (provider == null) {
            throw new IllegalArgumentException("unknown embedding provider: " + spec.provider());
        }
        Breaker breaker = breakers.computeIfAbsent(spec.provider(), p -> new Breaker());
        ProviderUnavailableException last = null;
        for (int attempt = 0; attempt < attempts; attempt++) {
            synchronized (breaker) {
                if (clock.instant().isBefore(breaker.openUntil)) {
                    throw new ProviderUnavailableException(spec.provider() + " circuit open until " + breaker.openUntil, last);
                }
            }
            if (attempt > 0) {
                sleeper.accept(Math.min(BACKOFF_CAP_MS, BACKOFF_BASE_MS << (attempt - 1)));
            }
            try {
                List<float[]> out = provider.embed(spec, texts);
                synchronized (breaker) {
                    breaker.consecutiveFailures = 0;
                }
                check(spec, texts, out);
                return out;
            } catch (ProviderUnavailableException e) {
                last = e;
                synchronized (breaker) {
                    if (++breaker.consecutiveFailures >= BREAKER_THRESHOLD) {
                        breaker.openUntil = clock.instant().plus(BREAKER_OPEN);
                    }
                }
            }
        }
        throw last;
    }

    private static void check(EmbeddingSpec spec, List<String> texts, List<float[]> out) {
        if (out.size() != texts.size()) {
            throw new IllegalStateException(spec.provider() + " returned " + out.size() + " vectors for " + texts.size() + " texts");
        }
        for (float[] v : out) {
            if (v == null || v.length != spec.dimensions()) {
                throw new IllegalStateException(spec.provider() + "/" + spec.model() + " returned a vector of "
                        + (v == null ? 0 : v.length) + " dimensions; the pipeline expects " + spec.dimensions());
            }
        }
    }

    private static void sleep(long ms) {
        try {
            Thread.sleep(ms);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new ProviderUnavailableException("interrupted while backing off");
        }
    }
}
