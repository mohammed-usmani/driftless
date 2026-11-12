package dev.driftless.server.change;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

class HashesTest {

    @Test
    void sha256IsHex() {
        assertThat(Hashes.sha256Hex("abc")).isEqualTo("ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad");
    }

    @Test
    void metadataHashIgnoresMapOrder() {
        Map<String, Object> a = new LinkedHashMap<>();
        a.put("category", "x");
        a.put("acl", List.of("u1"));
        Map<String, Object> b = new LinkedHashMap<>();
        b.put("acl", List.of("u1"));
        b.put("category", "x");
        assertThat(Hashes.metadata(a)).isEqualTo(Hashes.metadata(b));
    }

    @Test
    void metadataHashChangesWithAValue() {
        assertThat(Hashes.metadata(Map.of("acl", List.of("u1")))).isNotEqualTo(Hashes.metadata(Map.of("acl", List.of("u2"))));
    }
}
