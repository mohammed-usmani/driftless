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
    void uuidsRenderLowercaseFromStringsOrUuids() {
        UUID u = UUID.fromString("0f8fad5b-d9cb-469f-a165-70867728950e");
        assertThat(RowKeys.of(u.toString().toUpperCase())).isEqualTo(u.toString());
        assertThat(RowKeys.of(u)).isEqualTo(u.toString());
    }

    @Test
    void textKeysKeepTheirCase() {
        assertThat(RowKeys.of("Order-A")).isEqualTo("Order-A");
    }

    @Test
    void debeziumKeyJsonMatchesTheConverterOutput() {
        assertThat(RowKeys.debeziumKeyJson("id", 42L)).isEqualTo("{\"id\":42}");
        assertThat(RowKeys.debeziumKeyJson("id", UUID.fromString("0f8fad5b-d9cb-469f-a165-70867728950e")))
                .isEqualTo("{\"id\":\"0f8fad5b-d9cb-469f-a165-70867728950e\"}");
        assertThat(RowKeys.debeziumKeyJson("code", "Order-A")).isEqualTo("{\"code\":\"Order-A\"}");
    }
}
