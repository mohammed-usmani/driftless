package dev.driftless.server.change;

import static org.assertj.core.api.Assertions.assertThat;

import java.math.BigDecimal;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.List;
import org.junit.jupiter.api.Test;

class RowValuesTest {

    @Test
    void arraysFromDebeziumAndJdbcRenderEqual() throws Exception {
        java.sql.Array jdbc = new FakeArray(new String[] {"a", "b"});
        assertThat(RowValues.canonical(List.of("a", "b"))).isEqualTo(RowValues.canonical(jdbc));
    }

    @Test
    void numericStringAndBigDecimalRenderEqual() {
        assertThat(RowValues.canonical("12.50")).isEqualTo(RowValues.canonical(new BigDecimal("12.50")));
    }

    @Test
    void integersRenderEqualAcrossTypes() {
        assertThat(RowValues.canonical(7L)).isEqualTo(RowValues.canonical(7)).isEqualTo("7");
    }

    @Test
    void timestamptzFromDebeziumAndJdbcRenderEqual() {
        Instant t = Instant.parse("2026-11-12T16:57:27.154379Z");
        String debezium = "2026-11-12T16:57:27.154379Z";
        assertThat(RowValues.canonical(debezium))
                .isEqualTo(RowValues.canonical(OffsetDateTime.ofInstant(t, ZoneOffset.ofHours(5))))
                .isEqualTo(RowValues.canonical(Timestamp.from(t)));
    }

    @Test
    void nullIsDistinctFromEmptyAndFromTheWordNull() {
        assertThat(RowValues.canonical(null)).isNotEqualTo(RowValues.canonical("")).isNotEqualTo(RowValues.canonical("null"));
    }

    @Test
    void ordinaryTextIsUnchanged() {
        assertThat(RowValues.canonical("Hello, world")).isEqualTo("Hello, world");
    }

    /** Minimal java.sql.Array over a String[]. */
    record FakeArray(String[] values) implements java.sql.Array {
        public String getBaseTypeName() { return "text"; }
        public int getBaseType() { return java.sql.Types.VARCHAR; }
        public Object getArray() { return values; }
        public Object getArray(java.util.Map<String, Class<?>> map) { return values; }
        public Object getArray(long index, int count) { throw new UnsupportedOperationException(); }
        public Object getArray(long index, int count, java.util.Map<String, Class<?>> map) { throw new UnsupportedOperationException(); }
        public java.sql.ResultSet getResultSet() { throw new UnsupportedOperationException(); }
        public java.sql.ResultSet getResultSet(java.util.Map<String, Class<?>> map) { throw new UnsupportedOperationException(); }
        public java.sql.ResultSet getResultSet(long index, int count) { throw new UnsupportedOperationException(); }
        public java.sql.ResultSet getResultSet(long index, int count, java.util.Map<String, Class<?>> map) { throw new UnsupportedOperationException(); }
        public void free() { }
    }
}
