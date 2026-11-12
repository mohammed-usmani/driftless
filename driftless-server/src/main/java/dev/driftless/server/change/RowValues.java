package dev.driftless.server.change;

import java.math.BigDecimal;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.format.DateTimeParseException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.regex.Pattern;
import tools.jackson.databind.json.JsonMapper;

/**
 * One rendering per value for both paths a row arrives by: Debezium JSON (live changes) and JDBC (the
 * reconciler). If they ever disagree the reconciler reports drift that isn't there. Supported column
 * types are checked by preflight: text, numbers, booleans, uuid, timestamptz and arrays of those.
 */
public final class RowValues {

    /** Distinct from any text value, including "" and "null". */
    public static final String NULL = "\u0000";

    private static final Pattern ISO_INSTANT =
            Pattern.compile("^\\d{4}-\\d{2}-\\d{2}T\\d{2}:\\d{2}(:\\d{2}(\\.\\d+)?)?(Z|[+-]\\d{2}:\\d{2})$");
    private static final JsonMapper JSON = JsonMapper.builder().build();

    private RowValues() { }

    public static String canonical(Object v) {
        return switch (v) {
            case null -> NULL;
            case String s when ISO_INSTANT.matcher(s).matches() -> instant(s);
            case String s -> s;
            case BigDecimal d -> d.toPlainString();
            case Timestamp t -> t.toInstant().toString();
            case OffsetDateTime t -> t.toInstant().toString();
            case Instant i -> i.toString();
            case java.sql.Array a -> canonical(array(a));
            case Object[] arr -> canonical(Arrays.asList(arr));
            case List<?> list -> {
                List<String> out = new ArrayList<>(list.size());
                list.forEach(x -> out.add(canonical(x)));
                yield JSON.writeValueAsString(out);
            }
            default -> v.toString();
        };
    }

    private static String instant(String s) {
        try {
            return OffsetDateTime.parse(s).toInstant().toString();
        } catch (DateTimeParseException e) {
            return s;
        }
    }

    private static Object[] array(java.sql.Array a) {
        try {
            return (Object[]) a.getArray();
        } catch (SQLException e) {
            throw new IllegalStateException("could not read array value", e);
        }
    }
}
