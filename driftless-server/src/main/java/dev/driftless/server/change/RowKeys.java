package dev.driftless.server.change;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Map;
import java.util.UUID;
import tools.jackson.databind.json.JsonMapper;

/** One canonical string per row key, whether it came from a Debezium key or a JDBC read. */
public final class RowKeys {

    private static final JsonMapper JSON = JsonMapper.builder().build();

    private RowKeys() { }

    public static String of(Object key) {
        return switch (key) {
            case BigDecimal d -> d.toPlainString(); // keeps the scale, as Debezium's string does ("12.50")
            case Number n -> n.toString();
            case UUID u -> u.toString();
            default -> key.toString();
        };
    }

    /** The key exactly as Debezium's JSON converter writes it, so a record we produce lands on the same partition. */
    public static String debeziumKeyJson(String keyField, Object key) {
        Object json = switch (key) {
            case Long l -> l;
            case Integer i -> i;
            case Short s -> s;
            case BigInteger b -> b;
            default -> of(key);
        };
        return JSON.writeValueAsString(Map.of(keyField, json));
    }
}
