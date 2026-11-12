package dev.driftless.server.support;

import dev.driftless.server.change.Hashes;
import dev.driftless.server.change.RowKeys;
import dev.driftless.server.change.TextRenderer;
import dev.driftless.server.config.PipelineConfig;
import dev.driftless.server.generation.Generation;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.springframework.jdbc.core.JdbcTemplate;

/** Compares a generation against the source table it indexes. */
public final class Convergence {

    private Convergence() { }

    /** Everything about generation {@code g} that doesn't match the source, as readable lines. */
    public static List<String> differences(JdbcTemplate jdbc, JdbcTemplate sourceJdbc, PipelineConfig p, Generation g) {
        Map<String, Map<String, Object>> source = new HashMap<>();
        for (Map<String, Object> row : sourceJdbc.queryForList("SELECT * FROM " + p.table())) {
            source.put(RowKeys.of(row.get("id")), row);
        }
        Map<String, Map<String, Object>> states = new HashMap<>();
        for (Map<String, Object> s : jdbc.queryForList("SELECT * FROM row_state WHERE generation_id = ?", g.id())) {
            states.put((String) s.get("row_key"), s);
        }
        Map<String, List<Integer>> chunks = new HashMap<>();
        for (Map<String, Object> c : jdbc.queryForList("SELECT row_key, chunk_no FROM " + g.chunkTable() + " ORDER BY row_key, chunk_no")) {
            chunks.computeIfAbsent((String) c.get("row_key"), k -> new ArrayList<>()).add((Integer) c.get("chunk_no"));
        }

        List<String> diffs = new ArrayList<>();
        source.forEach((key, row) -> {
            Map<String, Object> s = states.get(key);
            if (s == null || (Boolean) s.get("deleted")) {
                diffs.add(key + ": in source but " + (s == null ? "not indexed" : "deleted in index"));
                return;
            }
            String doc = Hashes.sha256Hex(TextRenderer.render(p.text(), row));
            Map<String, Object> meta = new LinkedHashMap<>();
            p.metadata().forEach(c -> meta.put(c, row.get(c)));
            if (!doc.equals(s.get("doc_hash"))) {
                diffs.add(key + ": text differs");
            }
            if (!Hashes.metadata(meta).equals(s.get("meta_hash"))) {
                diffs.add(key + ": metadata differs");
            }
            int n = (Integer) s.get("chunk_count");
            List<Integer> expected = java.util.stream.IntStream.range(0, n).boxed().toList();
            if (!expected.equals(chunks.getOrDefault(key, List.of()))) {
                diffs.add(key + ": chunks " + chunks.getOrDefault(key, List.of()) + " but chunk_count " + n);
            }
        });
        states.forEach((key, s) -> {
            if (!source.containsKey(key) && !(Boolean) s.get("deleted")) {
                diffs.add(key + ": deleted in source but live in index");
            }
        });
        chunks.keySet().forEach(key -> {
            if (!source.containsKey(key)) {
                diffs.add(key + ": chunks left for a deleted row");
            }
        });
        return diffs;
    }
}
