package dev.driftless.server.source;

import dev.driftless.server.change.RowKeys;
import dev.driftless.server.config.PipelineConfig;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.springframework.stereotype.Component;

/** Direct reads of a pipeline's source table, for the reconciler. Table and column names come from validated config. */
@Component
public class SourceReader {

    private final SourceDatabase.Source source;

    public SourceReader(SourceDatabase.Source source) {
        this.source = source;
    }

    /** About n random rows; TABLESAMPLE on big tables, a full random sort on small ones. */
    public List<Map<String, Object>> sample(PipelineConfig p, int n) {
        Long estimate = source.jdbc().queryForObject("SELECT reltuples::bigint FROM pg_class WHERE oid = ?::regclass", Long.class, p.table());
        if (estimate == null || estimate < 10L * n) {
            return source.jdbc().queryForList("SELECT * FROM " + p.table() + " ORDER BY random() LIMIT ?", n);
        }
        double percent = Math.min(100, 200.0 * n / estimate); // twice what's needed: SYSTEM sampling is by page
        return source.jdbc().queryForList("SELECT * FROM " + p.table() + " TABLESAMPLE SYSTEM (" + percent + ") LIMIT ?", n);
    }

    /** The rows with these keys that exist now, by row key. */
    public Map<String, Map<String, Object>> rows(PipelineConfig p, Collection<String> keys) {
        Map<String, Map<String, Object>> out = new LinkedHashMap<>();
        if (keys.isEmpty()) {
            return out;
        }
        String sql = "SELECT * FROM %s WHERE %s = ANY (CAST(? AS %s[]))".formatted(p.table(), p.key(), keyType(p));
        for (Map<String, Object> row : source.jdbc().queryForList(sql, (Object) keys.toArray(String[]::new))) {
            out.put(RowKeys.of(row.get(p.key())), row);
        }
        return out;
    }

    public Set<String> existing(PipelineConfig p, Collection<String> keys) {
        return rows(p, keys).keySet();
    }

    /** The key column's SQL type, e.g. {@code bigint} or {@code uuid}. */
    public String keyType(PipelineConfig p) {
        return source.jdbc().queryForObject(
                "SELECT format_type(atttypid, atttypmod) FROM pg_attribute WHERE attrelid = ?::regclass AND attname = ?",
                String.class, p.table(), p.key());
    }

    public boolean integerKey(PipelineConfig p) {
        return Set.of("smallint", "integer", "bigint").contains(keyType(p));
    }
}
