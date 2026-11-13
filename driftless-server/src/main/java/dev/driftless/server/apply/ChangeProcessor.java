package dev.driftless.server.apply;

import dev.driftless.server.change.ChangeEvent;
import dev.driftless.server.change.Op;
import dev.driftless.server.embed.EmbedResult;
import dev.driftless.server.embed.EmbeddingService;
import dev.driftless.server.embed.EmbeddingSpec;
import dev.driftless.server.embed.ProviderUnavailableException;
import dev.driftless.server.generation.Generation;
import dev.driftless.server.generation.GenerationRepository;
import dev.driftless.server.generation.RowStateRepository;
import java.time.Clock;
import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.springframework.stereotype.Component;

/**
 * Applies a batch of one pipeline's changes to every generation that receives them. Classifies and embeds
 * without locks (embedding is slow), then applies each change in its own short locked transaction.
 * A truncate is a barrier: changes on either side of it are never merged.
 */
@Component
public class ChangeProcessor {

    private final GenerationRepository generations;
    private final RowStateRepository rows;
    private final Preparer preparer;
    private final EmbeddingService embeddings;
    private final ChangeApplier applier;
    private final FailedChangeRepository failed;
    private final RecentChangeRepository recent;
    private final dev.driftless.server.ops.DriftlessMetrics metrics;
    private final Clock clock;

    public ChangeProcessor(GenerationRepository generations, RowStateRepository rows, Preparer preparer,
                           EmbeddingService embeddings, ChangeApplier applier, FailedChangeRepository failed,
                           RecentChangeRepository recent, dev.driftless.server.ops.DriftlessMetrics metrics, Clock clock) {
        this.generations = generations;
        this.rows = rows;
        this.preparer = preparer;
        this.embeddings = embeddings;
        this.applier = applier;
        this.failed = failed;
        this.recent = recent;
        this.metrics = metrics;
        this.clock = clock;
    }

    private record Work(ChangeEvent event, Generation generation, Prepared prepared, Outcome expected) { }

    public List<Applied> process(String pipeline, List<ChangeEvent> batch) {
        List<Generation> targets = generations.targets(pipeline);
        if (targets.isEmpty() || batch.isEmpty()) {
            return List.of();
        }
        List<Applied> out = new ArrayList<>();
        List<ChangeEvent> segment = new ArrayList<>();
        for (ChangeEvent e : batch) {
            if (e.op() == Op.TRUNCATE) {
                out.addAll(segment(targets, segment));
                segment.clear();
                for (Generation g : targets) {
                    out.add(applied(g, e, applier.apply(g, e, Prepared.NONE, null), 0, 0));
                }
            } else {
                segment.add(e);
            }
        }
        out.addAll(segment(targets, segment));
        recent.record(out);
        metrics.applied(out);
        return out;
    }

    private List<Applied> segment(List<Generation> targets, List<ChangeEvent> events) {
        if (events.isEmpty()) {
            return List.of();
        }
        Map<String, ChangeEvent> newest = new LinkedHashMap<>();
        for (ChangeEvent e : events) {
            newest.merge(e.key(), e, (a, b) -> a.version().compareTo(b.version()) >= 0 ? a : b);
        }

        // ponytail: one row_state read per (change, generation); batch it if this shows up in profiles
        List<Work> work = new ArrayList<>();
        for (ChangeEvent e : newest.values()) {
            for (Generation g : targets) {
                Prepared p = preparer.prepare(g, e);
                work.add(new Work(e, g, p, Classifier.classify(e, p, rows.find(g.id(), e.key()))));
            }
        }

        // one embedding request per model for the whole segment
        Map<EmbeddingSpec, List<Work>> byModel = new LinkedHashMap<>();
        work.stream().filter(w -> w.expected() == Outcome.EMBED)
                .forEach(w -> byModel.computeIfAbsent(w.generation().embedding(), s -> new ArrayList<>()).add(w));
        Map<Work, List<float[]>> vectors = new java.util.IdentityHashMap<>();
        Map<Work, int[]> counts = new java.util.IdentityHashMap<>();
        Map<Work, String> rejected = new java.util.IdentityHashMap<>();
        try {
            byModel.forEach((spec, ws) -> {
                try {
                    embed(spec, ws, vectors, counts);
                } catch (IllegalArgumentException batchRejected) {
                    // the provider refused something in the batch and retrying won't help: find the rows it refuses
                    // ponytail: one call per row after a rejection; bisect if rejections become common
                    for (Work w : ws) {
                        try {
                            embed(spec, List.of(w), vectors, counts);
                        } catch (IllegalArgumentException e) {
                            rejected.put(w, e.getMessage());
                        }
                    }
                }
            });
        } catch (ProviderUnavailableException e) {
            // the batch will be redelivered; meanwhile deletes and ACL changes must not wait for the provider
            for (Work w : work) {
                if (w.expected() != Outcome.EMBED) {
                    applier.apply(w.generation(), w.event(), w.prepared(), null);
                }
            }
            throw e;
        }

        List<Applied> out = new ArrayList<>();
        Map<ChangeEvent, String> failures = new LinkedHashMap<>();
        for (Work w : work) {
            if (rejected.containsKey(w)) {
                out.add(applied(w.generation(), w.event(), Outcome.FAILED, 0, 0));
                failures.putIfAbsent(w.event(), rejected.get(w));
                continue;
            }
            Outcome o = applier.apply(w.generation(), w.event(), w.prepared(), vectors.get(w));
            int[] c = o == Outcome.EMBED ? counts.getOrDefault(w, new int[2]) : new int[2];
            out.add(applied(w.generation(), w.event(), o, c[0], c[1]));
            if (w.prepared().renderError() != null && o != Outcome.STALE) {
                failures.putIfAbsent(w.event(), w.prepared().renderError());
            }
        }
        failures.forEach((e, reason) -> failed.record(e.pipeline(), e.key(), e.version(), reason, e.row()));
        java.util.Set<String> succeeded = new java.util.HashSet<>();
        for (Applied a : out) {
            if (a.outcome() != Outcome.FAILED && a.outcome() != Outcome.STALE) {
                succeeded.add(a.key());
            }
        }
        failures.keySet().forEach(e -> succeeded.remove(e.key()));
        if (!succeeded.isEmpty()) {
            failed.resolve(events.getFirst().pipeline(), succeeded);
        }
        return out;
    }

    private void embed(EmbeddingSpec spec, List<Work> ws, Map<Work, List<float[]>> vectors, Map<Work, int[]> counts) {
        List<String> chunks = new ArrayList<>();
        ws.forEach(w -> chunks.addAll(w.prepared().chunks()));
        EmbedResult r = embeddings.embed(spec, chunks);
        long missedChars = 0;
        for (int i = 0; i < chunks.size(); i++) {
            missedChars += r.cached()[i] ? 0 : chunks.get(i).length();
        }
        metrics.embedded(ws.getFirst().event().pipeline(), spec.model(), r.cacheHits(), r.embedded(), r.providerCalls(), missedChars);
        int at = 0;
        for (Work w : ws) {
            int n = w.prepared().chunks().size();
            vectors.put(w, r.vectors().subList(at, at + n));
            int hits = 0;
            for (int i = at; i < at + n; i++) {
                hits += r.cached()[i] ? 1 : 0;
            }
            counts.put(w, new int[] {n - hits, hits});
            at += n;
        }
    }

    private Applied applied(Generation g, ChangeEvent e, Outcome o, int embedded, int hits) {
        long freshness = Math.max(0, Duration.between(e.committedAt(), clock.instant()).toMillis());
        return new Applied(e.pipeline(), g.id(), e.key(), o, embedded, hits, freshness, e.snapshot());
    }
}
