package dev.driftless.server.apply;

import dev.driftless.server.change.ChangeEvent;
import dev.driftless.server.generation.RowState;
import java.util.Optional;

/** Decides what a change does to a generation, given what that generation last applied for the row. */
public final class Classifier {

    private Classifier() { }

    public static Outcome classify(ChangeEvent e, Prepared p, Optional<RowState> current) {
        if (current.isPresent() && e.version().compareTo(current.get().version()) <= 0) {
            return Outcome.STALE;
        }
        switch (e.op()) {
            case DELETE -> {
                return Outcome.DELETE;
            }
            case TRUNCATE -> {
                return Outcome.TRUNCATE;
            }
            default -> { }
        }
        boolean indexed = current.isPresent() && !current.get().deleted();
        if (p.renderError() != null) {
            // the text can't be rendered, but an access change must still land
            boolean metadataChanged = p.metaHash() != null && indexed && !p.metaHash().equals(current.get().metaHash());
            return metadataChanged ? Outcome.METADATA_ONLY : Outcome.FAILED;
        }
        if (!indexed || current.get().textStale() || !p.docHash().equals(current.get().docHash())) {
            return Outcome.EMBED;
        }
        return p.metaHash().equals(current.get().metaHash()) ? Outcome.SKIP : Outcome.METADATA_ONLY;
    }
}
