package dev.driftless.server.apply;

import dev.driftless.server.change.ChangeEvent;
import dev.driftless.server.change.Chunker;
import dev.driftless.server.change.Hashes;
import dev.driftless.server.change.Op;
import dev.driftless.server.change.RenderException;
import dev.driftless.server.change.TextRenderer;
import dev.driftless.server.generation.Generation;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.springframework.stereotype.Component;

@Component
public class Preparer {

    public Prepared prepare(Generation g, ChangeEvent e) {
        if (e.op() != Op.UPSERT) {
            return Prepared.NONE;
        }
        Map<String, Object> metadata = new LinkedHashMap<>();
        String metadataError = null;
        for (String col : g.metadataColumns()) {
            if (!e.row().containsKey(col)) {
                metadataError = "unknown column: " + col;
                break;
            }
            metadata.put(col, e.row().get(col));
        }
        String metaHash = metadataError == null ? Hashes.metadata(metadata) : null;
        Map<String, Object> meta = metadataError == null ? metadata : null;
        try {
            String text = TextRenderer.render(g.template(), e.row());
            if (metadataError != null) {
                return new Prepared(null, null, null, null, null, metadataError);
            }
            // a row with nothing to say is indexed with no chunks, never as an embedding of whitespace
            List<String> chunks = text.isBlank() ? List.of() : Chunker.split(text, g.maxChars(), g.overlapChars());
            return new Prepared(text, Hashes.sha256Hex(text), meta, metaHash, chunks, null);
        } catch (RenderException ex) {
            return new Prepared(null, null, meta, metaHash, null, ex.getMessage());
        }
    }
}
