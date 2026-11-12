package dev.driftless.server.source;

import dev.driftless.server.config.PipelineConfig;
import java.util.ArrayList;
import dev.driftless.server.change.TextRenderer;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;

/** Checks that a source table can be synced safely. Every problem is a sentence an operator can act on. */
@Component
public class SourcePreflight {

    public static final String SIGNAL_TABLE = "driftless_signal";

    private final JdbcTemplate source;

    @Autowired
    public SourcePreflight(SourceDatabase.Source source) {
        this(source.jdbc());
    }

    public SourcePreflight(JdbcTemplate source) {
        this.source = source;
    }

    public List<String> check(PipelineConfig p) {
        List<String> problems = new ArrayList<>();
        String walLevel = source.queryForObject("SHOW wal_level", String.class);
        if (!"logical".equals(walLevel)) {
            problems.add("wal_level is " + walLevel + "; logical replication needs wal_level=logical");
        }
        String table = p.schema() + "." + p.tableName();
        String signal = p.schema() + "." + SIGNAL_TABLE;
        String publication = publication(p);
        boolean pubExists = Boolean.TRUE.equals(source.queryForObject(
                "SELECT exists(SELECT 1 FROM pg_publication WHERE pubname = ?)", Boolean.class, publication));
        if (!pubExists) {
            problems.add("publication " + publication + " does not exist; create it FOR TABLE " + table + ", " + signal);
        }

        if (exists(table)) {
            String identity = source.queryForObject(
                    "SELECT relreplident::text FROM pg_class WHERE oid = ?::regclass", String.class, table);
            if (!"f".equals(identity)) {
                problems.add("table " + table + " must have REPLICA IDENTITY FULL (it has " + identityName(identity)
                        + "), or updates can arrive without their large text columns");
            }
            if (pubExists && !inPublication(publication, p.schema(), p.tableName())) {
                problems.add("table " + table + " is not in publication " + publication);
            }
            problems.addAll(columnProblems(p, table));
        } else {
            problems.add("table " + table + " does not exist");
        }

        if (exists(signal)) {
            if (pubExists && !inPublication(publication, p.schema(), SIGNAL_TABLE)) {
                problems.add("signal table " + signal + " is not in publication " + publication
                        + "; without it incremental snapshots never run");
            }
            boolean canInsert = Boolean.TRUE.equals(source.queryForObject(
                    "SELECT has_table_privilege(current_user, ?, 'INSERT')", Boolean.class, signal));
            if (!canInsert) {
                problems.add("the source user cannot INSERT into " + signal + "; Driftless writes snapshot signals there");
            }
        } else {
            problems.add("signal table " + signal + " does not exist");
        }
        return problems;
    }

    /**
     * Columns the template and metadata use must exist and be of a type that renders the same from
     * Debezium JSON and from JDBC (see RowValues); anything else would show up as false drift.
     */
    private List<String> columnProblems(PipelineConfig p, String table) {
        Map<String, String> types = new HashMap<>();
        source.query("""
                SELECT a.attname, t.typname FROM pg_attribute a JOIN pg_type t ON t.oid = a.atttypid
                WHERE a.attrelid = ?::regclass AND a.attnum > 0 AND NOT a.attisdropped""",
                rs -> { types.put(rs.getString(1), rs.getString(2)); }, table);
        List<String> problems = new ArrayList<>();
        LinkedHashSet<String> used = new LinkedHashSet<>(TextRenderer.columns(p.text()));
        used.addAll(p.metadata());
        used.add(p.key());
        for (String col : used) {
            String type = types.get(col);
            if (type == null) {
                problems.add("column " + col + " used by the pipeline does not exist in " + table);
            } else if (!SUPPORTED_TYPES.contains(type)) {
                problems.add("column " + col + " has type " + type + ", which Driftless cannot render consistently; supported: "
                        + String.join(", ", new java.util.TreeSet<>(SUPPORTED_TYPES)));
            }
        }
        return problems;
    }

    private static final Set<String> SUPPORTED_TYPES = Set.of(
            "text", "varchar", "bpchar", "int2", "int4", "int8", "numeric", "bool", "uuid", "timestamptz",
            "_text", "_varchar", "_int4", "_int8", "_uuid");

    public static String publication(PipelineConfig p) {
        return "driftless_" + p.name();
    }

    private boolean exists(String qualified) {
        return source.queryForObject("SELECT to_regclass(?) IS NOT NULL", Boolean.class, qualified);
    }

    private boolean inPublication(String publication, String schema, String table) {
        return Boolean.TRUE.equals(source.queryForObject("""
                SELECT exists(SELECT 1 FROM pg_publication_tables
                              WHERE pubname = ? AND schemaname = ? AND tablename = ?)""",
                Boolean.class, publication, schema, table));
    }

    private static String identityName(String code) {
        return switch (code) {
            case "d" -> "DEFAULT";
            case "n" -> "NOTHING";
            case "i" -> "USING INDEX";
            default -> code;
        };
    }
}
