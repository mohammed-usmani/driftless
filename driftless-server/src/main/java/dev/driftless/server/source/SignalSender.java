package dev.driftless.server.source;

import dev.driftless.server.config.PipelineConfig;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;
import org.springframework.stereotype.Component;
import tools.jackson.databind.json.JsonMapper;

/** Writes Debezium signals into the source's signal table. */
@Component
public class SignalSender {

    private static final JsonMapper JSON = JsonMapper.builder().build();

    private final SourceDatabase.Source source;

    public SignalSender(SourceDatabase.Source source) {
        this.source = source;
    }

    /**
     * Starts an incremental snapshot of the pipeline's table, or only of {@code keys} when given.
     * Keys go into the filter as SQL string literals; Postgres coerces them to the key's type.
     */
    public String executeSnapshot(PipelineConfig p, List<String> keys) {
        String collection = p.schema() + "." + p.tableName();
        Map<String, Object> data = new java.util.LinkedHashMap<>();
        data.put("data-collections", List.of(collection));
        data.put("type", "incremental");
        if (!keys.isEmpty()) {
            String in = keys.stream().map(k -> "'" + k.replace("'", "''") + "'").collect(Collectors.joining(","));
            data.put("additional-conditions",
                    List.of(Map.of("data-collection", collection, "filter", p.key() + " IN (" + in + ")")));
        }
        String id = UUID.randomUUID().toString();
        source.jdbc().update("INSERT INTO " + p.schema() + "." + SourcePreflight.SIGNAL_TABLE
                + " (id, type, data) VALUES (?, 'execute-snapshot', ?)", id, JSON.writeValueAsString(data));
        return id;
    }
}
