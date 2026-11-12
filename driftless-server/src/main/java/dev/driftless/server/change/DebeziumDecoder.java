package dev.driftless.server.change;

import dev.driftless.server.source.SourcePreflight;
import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.springframework.stereotype.Component;
import tools.jackson.core.JacksonException;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.json.JsonMapper;

/** Turns a Debezium record (JSON converter, schemas disabled) into a {@link ChangeEvent}. */
@Component
public class DebeziumDecoder {

    private static final JsonMapper JSON = JsonMapper.builder().build();

    /** Empty for tombstones and for records of the signal table. */
    public Optional<ChangeEvent> decode(String pipeline, String keyField, byte[] key, byte[] value) {
        if (value == null) {
            return Optional.empty();
        }
        JsonNode v;
        try {
            v = JSON.readTree(value);
        } catch (JacksonException e) {
            throw new DecodeException("record value is not JSON", e);
        }
        JsonNode source = v.path("source");
        if (SourcePreflight.SIGNAL_TABLE.equals(source.path("table").asString(null))) {
            return Optional.empty();
        }
        String debeziumOp = v.path("op").asString("");
        Op op = switch (debeziumOp) {
            case "c", "u", "r" -> Op.UPSERT;
            case "d" -> Op.DELETE;
            case "t" -> Op.TRUNCATE;
            default -> throw new DecodeException("unknown op '" + debeziumOp + "'", null);
        };
        Version version = Version.fromSource(source, debeziumOp);
        Instant committedAt = Instant.ofEpochMilli(source.path("ts_ms").asLong());
        boolean synthetic = v.at("/driftless/synthetic").asBoolean(false);
        if (op == Op.TRUNCATE) {
            return Optional.of(new ChangeEvent(pipeline, op, null, null, version, committedAt, false, false));
        }
        Object keyValue = keyValue(keyField, key, op == Op.DELETE ? v.path("before") : v.path("after"));
        Map<String, Object> row = op == Op.UPSERT ? toMap(v.path("after")) : null;
        return Optional.of(new ChangeEvent(pipeline, op, RowKeys.of(keyValue), row, version, committedAt,
                debeziumOp.equals("r"), synthetic));
    }

    private static Object keyValue(String keyField, byte[] key, JsonNode image) {
        JsonNode k = null;
        if (key != null) {
            try {
                k = JSON.readTree(key).get(keyField);
            } catch (JacksonException e) {
                throw new DecodeException("record key is not JSON", e);
            }
        }
        if (k == null || k.isNull()) {
            k = image.get(keyField);
        }
        if (k == null || k.isNull()) {
            throw new DecodeException("record has no value for key column '" + keyField + "'", null);
        }
        return toJava(k);
    }

    private static Map<String, Object> toMap(JsonNode obj) {
        Map<String, Object> m = new LinkedHashMap<>();
        for (Map.Entry<String, JsonNode> e : obj.properties()) {
            m.put(e.getKey(), toJava(e.getValue()));
        }
        return m;
    }

    static Object toJava(JsonNode n) {
        if (n == null || n.isNull()) {
            return null;
        }
        if (n.isString()) {
            return n.asString();
        }
        if (n.isIntegralNumber()) {
            return n.canConvertToLong() ? (Object) n.asLong() : n.bigIntegerValue();
        }
        if (n.isNumber()) {
            return n.asDouble();
        }
        if (n.isBoolean()) {
            return n.asBoolean();
        }
        if (n.isArray()) {
            List<Object> list = new ArrayList<>();
            n.forEach(x -> list.add(toJava(x)));
            return list;
        }
        return toMap(n);
    }
}
