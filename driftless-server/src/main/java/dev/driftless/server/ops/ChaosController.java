package dev.driftless.server.ops;

import dev.driftless.server.api.NotFoundException;
import dev.driftless.server.config.DriftlessProperties;
import dev.driftless.server.config.PipelineConfig;
import dev.driftless.server.generation.Generation;
import dev.driftless.server.generation.GenerationRepository;
import dev.driftless.server.source.SourceReader;
import jakarta.validation.Valid;
import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;
import org.springframework.context.annotation.Profile;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

/** Breaks the live index on purpose so the reconciler can be watched finding and fixing it. Dev and demo only. */
@Profile({"dev", "demo"})
@RestController
class ChaosController {

    record Count(@Min(1) @Max(1000) int count) { }

    private final DriftlessProperties props;
    private final GenerationRepository generations;
    private final SourceReader reader;
    private final JdbcTemplate jdbc;

    ChaosController(DriftlessProperties props, GenerationRepository generations, SourceReader reader, JdbcTemplate jdbc) {
        this.props = props;
        this.generations = generations;
        this.reader = reader;
        this.jdbc = jdbc;
    }

    /** Overwrites the text of N random indexed rows, as if an update had been lost. */
    @PostMapping("/v1/chaos/pipelines/{name}/corrupt")
    @Transactional
    Map<String, List<String>> corrupt(@PathVariable String name, @Valid @RequestBody Count body) {
        Generation g = live(name);
        List<String> keys = jdbc.queryForList("""
                UPDATE row_state SET doc_hash = 'corrupted-by-chaos' WHERE generation_id = ? AND row_key IN (
                    SELECT row_key FROM row_state WHERE generation_id = ? AND NOT deleted AND chunk_count > 0 ORDER BY random() LIMIT ?)
                RETURNING row_key""", String.class, g.id(), g.id(), body.count());
        jdbc.update("UPDATE " + g.chunkTable() + " SET text = 'corrupted by chaos' WHERE row_key = ANY (?)", (Object) keys.toArray(String[]::new));
        return Map.of("keys", keys);
    }

    /**
     * Indexes N rows that don't exist in the source, as if their deletes had been lost. (A real source delete
     * can't be hidden from logical decoding, so the leak is made on the index side.)
     */
    @PostMapping("/v1/chaos/pipelines/{name}/leak")
    @Transactional
    Map<String, List<String>> leak(@PathVariable String name, @Valid @RequestBody Count body) {
        Generation g = live(name);
        boolean integer = reader.integerKey(props.pipeline(name));
        List<Double> unit = new ArrayList<>(Collections.nCopies(g.embedding().dimensions(), 0.0));
        unit.set(0, 1.0);
        List<String> keys = new ArrayList<>();
        for (int i = 0; i < body.count(); i++) {
            String key = integer ? Long.toString(9_000_000_000L + ThreadLocalRandom.current().nextLong(1_000_000_000L)) : UUID.randomUUID().toString();
            jdbc.update("""
                    INSERT INTO row_state (generation_id, row_key, version_major, version_minor, doc_hash, meta_hash, chunk_count,
                                           deleted, text_stale, committed_at, applied_at)
                    VALUES (?, ?, 1, 0, 'leaked-by-chaos', 'leaked-by-chaos', 1, false, false, now(), now())""", g.id(), key);
            jdbc.update("INSERT INTO " + g.chunkTable() + " (row_key, chunk_no, text, chunk_hash, embedding, metadata, version_major, version_minor) "
                    + "VALUES (?, 0, 'leaked by chaos', 'leaked', ?::vector, '{}', 1, 0)", key, unit.toString());
            keys.add(key);
        }
        return Map.of("keys", keys);
    }

    private Generation live(String name) {
        PipelineConfig p = props.pipeline(name);
        if (p == null) {
            throw new NotFoundException("no pipeline " + name);
        }
        return generations.live(name).orElseThrow(() -> new NotFoundException("pipeline " + name + " has no live generation"));
    }
}
