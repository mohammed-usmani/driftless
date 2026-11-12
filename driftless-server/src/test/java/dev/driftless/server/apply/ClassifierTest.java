package dev.driftless.server.apply;

import static dev.driftless.server.apply.Outcome.DELETE;
import static dev.driftless.server.apply.Outcome.EMBED;
import static dev.driftless.server.apply.Outcome.FAILED;
import static dev.driftless.server.apply.Outcome.METADATA_ONLY;
import static dev.driftless.server.apply.Outcome.SKIP;
import static dev.driftless.server.apply.Outcome.STALE;
import static dev.driftless.server.apply.Outcome.TRUNCATE;
import static org.assertj.core.api.Assertions.assertThat;

import dev.driftless.server.change.ChangeEvent;
import dev.driftless.server.change.Op;
import dev.driftless.server.change.Version;
import dev.driftless.server.generation.RowState;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.junit.jupiter.api.Test;

class ClassifierTest {

    private static ChangeEvent event(Op op, long version) {
        return new ChangeEvent("p", op, op == Op.TRUNCATE ? null : "1", op == Op.UPSERT ? Map.of() : null,
                new Version(version, 0), Instant.EPOCH, false, false);
    }

    private static Prepared upsert(String doc, String meta) {
        return new Prepared("text", doc, Map.of(), meta, List.of("text"), null);
    }

    private static Optional<RowState> state(long version, String doc, String meta, boolean deleted) {
        return Optional.of(new RowState(1, "1", new Version(version, 0), doc, meta, 1, deleted, false, Instant.EPOCH));
    }

    @Test
    void olderVersionIsStale() {
        assertThat(Classifier.classify(event(Op.UPSERT, 4), upsert("d", "m"), state(5, "d", "m", false))).isEqualTo(STALE);
    }

    @Test
    void equalVersionIsStale() {
        assertThat(Classifier.classify(event(Op.UPSERT, 5), upsert("x", "y"), state(5, "d", "m", false))).isEqualTo(STALE);
    }

    @Test
    void deleteAndTruncateFollowTheirOp() {
        assertThat(Classifier.classify(event(Op.DELETE, 6), Prepared.NONE, state(5, "d", "m", false))).isEqualTo(DELETE);
        assertThat(Classifier.classify(event(Op.TRUNCATE, 6), Prepared.NONE, Optional.empty())).isEqualTo(TRUNCATE);
    }

    @Test
    void newRowEmbeds() {
        assertThat(Classifier.classify(event(Op.UPSERT, 1), upsert("d", "m"), Optional.empty())).isEqualTo(EMBED);
    }

    @Test
    void unchangedHashesSkip() {
        assertThat(Classifier.classify(event(Op.UPSERT, 6), upsert("d", "m"), state(5, "d", "m", false))).isEqualTo(SKIP);
    }

    @Test
    void onlyMetadataChangedIsMetadataOnly() {
        assertThat(Classifier.classify(event(Op.UPSERT, 6), upsert("d", "m2"), state(5, "d", "m", false))).isEqualTo(METADATA_ONLY);
    }

    @Test
    void textChangedEmbeds() {
        assertThat(Classifier.classify(event(Op.UPSERT, 6), upsert("d2", "m"), state(5, "d", "m", false))).isEqualTo(EMBED);
    }

    @Test
    void upsertAfterDeleteEmbeds() {
        assertThat(Classifier.classify(event(Op.UPSERT, 6), upsert("d", "m"), state(5, "d", "m", true))).isEqualTo(EMBED);
    }

    @Test
    void staleTextIsReEmbeddedEvenIfTheHashMatches() {
        Optional<RowState> stale = Optional.of(new RowState(1, "1", new Version(5, 0), "d", "m", 1, false, true, Instant.EPOCH));
        assertThat(Classifier.classify(event(Op.UPSERT, 6), upsert("d", "m"), stale)).isEqualTo(EMBED);
    }

    @Test
    void renderErrorWithAclChangeIsMetadataOnly() {
        Prepared broken = new Prepared(null, null, Map.of("acl", List.of("u2")), "m2", null, "unavailable value in column: body");
        assertThat(Classifier.classify(event(Op.UPSERT, 6), broken, state(5, "d", "m", false))).isEqualTo(METADATA_ONLY);
    }

    @Test
    void renderErrorWithoutMetadataChangeFails() {
        Prepared broken = new Prepared(null, null, Map.of(), "m", null, "unavailable value in column: body");
        assertThat(Classifier.classify(event(Op.UPSERT, 6), broken, state(5, "d", "m", false))).isEqualTo(FAILED);
    }

    @Test
    void renderErrorOnARowNotIndexedFails() {
        Prepared broken = new Prepared(null, null, Map.of(), "m2", null, "unknown column: body");
        assertThat(Classifier.classify(event(Op.UPSERT, 6), broken, Optional.empty())).isEqualTo(FAILED);
        assertThat(Classifier.classify(event(Op.UPSERT, 6), broken, state(5, null, null, true))).isEqualTo(FAILED);
    }
}
