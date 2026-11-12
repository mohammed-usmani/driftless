package dev.driftless.server.apply;

import java.util.List;
import java.util.Map;

/**
 * A change rendered for one generation. For an upsert whose text could not be rendered, {@code renderError}
 * is set and the text fields are null; metadata is still filled when every metadata column was present.
 */
public record Prepared(String text, String docHash, Map<String, Object> metadata, String metaHash,
                       List<String> chunks, String renderError) {

    /** Deletes and truncates carry nothing. */
    public static final Prepared NONE = new Prepared(null, null, null, null, null, null);
}
