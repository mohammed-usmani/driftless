package dev.driftless.server.change;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.json.JsonMapper;

class VersionTest {

    @Test
    void comparesMajorThenMinor() {
        assertThat(new Version(2, 0)).isGreaterThan(new Version(1, 999));
        assertThat(new Version(1, 5)).isGreaterThan(new Version(1, 4));
        assertThat(new Version(1, 5)).isEqualByComparingTo(new Version(1, 5));
    }

    @Test
    void justAfterSitsBetweenAVersionAndAnyLaterChange() {
        Version v = new Version(100, 200);
        assertThat(v.justAfter()).isGreaterThan(v);
        assertThat(v.justAfter()).isLessThan(new Version(100, 224));
        assertThat(v.justAfter()).isLessThan(new Version(101, 0));
    }

    @Test
    void printsAsMajorSlashMinor() {
        assertThat(new Version(7, 9)).hasToString("7/9");
    }

    private static JsonNode source(String sequence) {
        return JsonMapper.builder().build().readTree("{\"sequence\":" + sequence + "}");
    }

    @Test
    void streamedChangeUsesLastCommitThenLsn() {
        assertThat(Version.fromSource(source("\"[\\\"32105048\\\",\\\"32083760\\\"]\""), "u"))
                .isEqualTo(new Version(32105048, 32083760));
    }

    @Test
    void snapshotReadUsesTheStreamPositionItWasEmittedAt() {
        assertThat(Version.fromSource(source("\"[\\\"115663336\\\",\\\"115663336\\\"]\""), "r"))
                .isEqualTo(new Version(115663336, 115663336));
    }

    @Test
    void missingLastCommitCountsAsZero() {
        assertThat(Version.fromSource(source("\"[null,\\\"32038976\\\"]\""), "c"))
                .isEqualTo(new Version(0, 32038976));
    }
}
