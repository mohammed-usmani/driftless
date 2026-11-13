package dev.driftless.server.change;

import static org.assertj.core.api.Assertions.assertThat;

import java.math.BigDecimal;
import java.util.UUID;
import org.junit.jupiter.api.Test;

class RowKeysTest {

    @Test
    void numbersRenderPlainWhateverTheirJavaType() {
        assertThat(RowKeys.of(42L)).isEqualTo("42");
        assertThat(RowKeys.of(42)).isEqualTo("42");
        assertThat(RowKeys.of(new BigDecimal("42"))).isEqualTo("42");
    }

    @Test
    void uuidsRenderAsPostgresWritesThem() {
        UUID u = UUID.fromString("0f8fad5b-d9cb-469f-a165-70867728950e");
        assertThat(RowKeys.of(u)).isEqualTo("0f8fad5b-d9cb-469f-a165-70867728950e");
    }

    @Test
    void textKeysKeepTheirCaseEvenWhenShapedLikeAUuid() {
        assertThat(RowKeys.of("Order-A")).isEqualTo("Order-A");
        // a text key, not a uuid column: the source matches it case-sensitively, so must we
        assertThat(RowKeys.of("6F9619FF-8B86-D011-B42D-00C04FC964FF")).isEqualTo("6F9619FF-8B86-D011-B42D-00C04FC964FF");
    }

    @Test
    void numericKeysKeepTheirScaleAsDebeziumSendsIt() {
        // decimal.handling.mode=string sends "12.50" for numeric(10,2); JDBC returns BigDecimal 12.50
        assertThat(RowKeys.of(new BigDecimal("12.50"))).isEqualTo("12.50");
        assertThat(RowKeys.of("12.50")).isEqualTo("12.50");
    }

    @Test
    void debeziumKeyJsonMatchesTheConverterOutput() {
        assertThat(RowKeys.debeziumKeyJson("id", 42L)).isEqualTo("{\"id\":42}");
        assertThat(RowKeys.debeziumKeyJson("id", UUID.fromString("0f8fad5b-d9cb-469f-a165-70867728950e")))
                .isEqualTo("{\"id\":\"0f8fad5b-d9cb-469f-a165-70867728950e\"}");
        assertThat(RowKeys.debeziumKeyJson("code", "Order-A")).isEqualTo("{\"code\":\"Order-A\"}");
    }
}
