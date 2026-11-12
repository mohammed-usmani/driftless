package dev.driftless.server.change;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import org.junit.jupiter.api.Test;

class DebeziumDecoderTest {

    private final DebeziumDecoder decoder = new DebeziumDecoder();

    private static byte[] fixture(String name) throws IOException {
        try (InputStream in = DebeziumDecoderTest.class.getResourceAsStream("/debezium/" + name + ".json")) {
            return in.readAllBytes();
        }
    }

    private static byte[] key(long id) {
        return ("{\"id\":" + id + "}").getBytes(StandardCharsets.UTF_8);
    }

    @Test
    void createIsAnUpsertWithItsRow() throws IOException {
        ChangeEvent e = decoder.decode("vo", "id", key(19), fixture("c")).orElseThrow();
        assertThat(e.op()).isEqualTo(Op.UPSERT);
        assertThat(e.key()).isEqualTo("19");
        assertThat(e.row()).containsKey("title").containsKey("body");
        assertThat(e.snapshot()).isFalse();
        assertThat(e.synthetic()).isFalse();
        var source = tools.jackson.databind.json.JsonMapper.builder().build().readTree(fixture("c")).path("source");
        assertThat(e.committedAt()).isEqualTo(Instant.ofEpochMilli(source.path("ts_ms").asLong()));
        assertThat(e.version()).isEqualTo(Version.fromSource(source, "c"));
    }

    @Test
    void updateIsAnUpsert() throws IOException {
        ChangeEvent e = decoder.decode("vo", "id", key(3), fixture("u")).orElseThrow();
        assertThat(e.op()).isEqualTo(Op.UPSERT);
        assertThat(e.row()).isNotNull();
    }

    @Test
    void deleteHasItsKeyAndNoRow() throws IOException {
        ChangeEvent e = decoder.decode("vo", "id", key(42), fixture("d")).orElseThrow();
        assertThat(e.op()).isEqualTo(Op.DELETE);
        assertThat(e.key()).isEqualTo("42");
        assertThat(e.row()).isNull();
    }

    @Test
    void snapshotReadIsAnUpsertMarkedAsSnapshot() throws IOException {
        ChangeEvent e = decoder.decode("vo", "id", key(1), fixture("r")).orElseThrow();
        assertThat(e.op()).isEqualTo(Op.UPSERT);
        assertThat(e.snapshot()).isTrue();
    }

    @Test
    void tombstoneIsIgnored() {
        assertThat(decoder.decode("vo", "id", key(1), null)).isEmpty();
    }

    @Test
    void signalTableRecordIsIgnored() {
        String v = """
                {"op":"c","after":{"id":"x","type":"execute-snapshot","data":"{}"},
                 "source":{"table":"driftless_signal","ts_ms":1,"sequence":"[null,\\"5\\"]"}}""";
        assertThat(decoder.decode("vo", "id", "{\"id\":\"x\"}".getBytes(), v.getBytes())).isEmpty();
    }

    @Test
    void truncateHasNoKey() {
        String v = """
                {"op":"t","before":null,"after":null,
                 "source":{"table":"vo_articles","ts_ms":1,"sequence":"[\\"9\\",\\"10\\"]"}}""";
        ChangeEvent e = decoder.decode("vo", "id", null, v.getBytes()).orElseThrow();
        assertThat(e.op()).isEqualTo(Op.TRUNCATE);
        assertThat(e.key()).isNull();
        assertThat(e.version()).isEqualTo(new Version(9, 10));
    }

    @Test
    void syntheticDeleteIsMarked() {
        String v = """
                {"op":"d","before":{"id":7},"source":{"table":"vo_articles","ts_ms":1,"sequence":"[\\"9\\",\\"11\\"]"},
                 "driftless":{"synthetic":true}}""";
        ChangeEvent e = decoder.decode("vo", "id", key(7), v.getBytes()).orElseThrow();
        assertThat(e.op()).isEqualTo(Op.DELETE);
        assertThat(e.synthetic()).isTrue();
    }

    @Test
    void malformedValueThrows() {
        assertThatThrownBy(() -> decoder.decode("vo", "id", key(1), "{not json".getBytes()))
                .isInstanceOf(DecodeException.class);
    }

    @Test
    void arraysBecomeLists() {
        String v = """
                {"op":"c","after":{"id":1,"acl":["a","b"],"price":"12.50"},
                 "source":{"table":"vo_articles","ts_ms":1,"sequence":"[\\"9\\",\\"10\\"]"}}""";
        ChangeEvent e = decoder.decode("vo", "id", key(1), v.getBytes()).orElseThrow();
        assertThat(e.row().get("acl")).isEqualTo(java.util.List.of("a", "b"));
        assertThat(e.row().get("price")).isEqualTo("12.50");
    }
}
