package dev.driftless.server.source;

import dev.driftless.server.config.DriftlessProperties;
import dev.driftless.server.config.PipelineConfig;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Component;
import org.springframework.web.client.HttpClientErrorException;
import org.springframework.web.client.RestClient;
import tools.jackson.databind.JsonNode;

/** Keeps one Debezium connector per pipeline configured in Kafka Connect. */
@Component
public class ConnectorManager {

    public enum State { RUNNING, STARTING, FAILED, MISSING }

    private final RestClient connect;
    private final SourceDatabase.Source source;
    private final DriftlessProperties props;

    public ConnectorManager(DriftlessProperties props, SourceDatabase.Source source) {
        this.props = props;
        this.source = source;
        this.connect = RestClient.builder().baseUrl(props.kafkaConnectUrl()).build();
    }

    public static String connectorName(String pipeline) {
        return "driftless-" + pipeline;
    }

    public String topic(PipelineConfig p) {
        return "driftless." + p.name() + "." + p.schema() + "." + p.tableName();
    }

    public String notificationTopic(PipelineConfig p) {
        return "driftless." + p.name() + ".notifications";
    }

    /** Creates or updates the connector. PUT on the config is idempotent. */
    public void ensure(PipelineConfig p) {
        connect.put().uri("/connectors/{name}/config", connectorName(p.name()))
                .body(config(p)).retrieve().toBodilessEntity();
    }

    public State state(String pipeline) {
        JsonNode status;
        try {
            status = connect.get().uri("/connectors/{name}/status", connectorName(pipeline))
                    .retrieve().body(JsonNode.class);
        } catch (HttpClientErrorException e) {
            if (e.getStatusCode() == HttpStatus.NOT_FOUND) {
                return State.MISSING;
            }
            throw e;
        }
        String connector = status.at("/connector/state").asString();
        JsonNode tasks = status.path("tasks");
        if ("FAILED".equals(connector)) {
            return State.FAILED;
        }
        for (JsonNode t : tasks) {
            if ("FAILED".equals(t.path("state").asString())) {
                return State.FAILED;
            }
        }
        boolean running = "RUNNING".equals(connector) && !tasks.isEmpty()
                && "RUNNING".equals(tasks.get(0).path("state").asString());
        return running ? State.RUNNING : State.STARTING;
    }

    public List<String> names() {
        return List.of(connect.get().uri("/connectors").retrieve().body(String[].class));
    }

    Map<String, String> config(PipelineConfig p) {
        DriftlessProperties.Source s = props.source();
        Map<String, String> c = new LinkedHashMap<>();
        c.put("connector.class", "io.debezium.connector.postgresql.PostgresConnector");
        c.put("plugin.name", "pgoutput");
        c.put("database.hostname", source.host());
        c.put("database.port", Integer.toString(source.port()));
        c.put("database.dbname", source.database());
        c.put("database.user", s.user());
        c.put("database.password", s.password());
        c.put("slot.name", "driftless_" + p.name());
        c.put("publication.name", SourcePreflight.publication(p));
        c.put("publication.autocreate.mode", "disabled");
        c.put("table.include.list", p.schema() + "." + p.tableName() + "," + p.schema() + "." + SourcePreflight.SIGNAL_TABLE);
        c.put("signal.data.collection", p.schema() + "." + SourcePreflight.SIGNAL_TABLE);
        c.put("topic.prefix", "driftless." + p.name());
        c.put("snapshot.mode", "no_data");
        c.put("decimal.handling.mode", "string");
        c.put("tombstones.on.delete", "false");
        c.put("notification.enabled.channels", "sink");
        c.put("notification.sink.topic.name", notificationTopic(p));
        c.put("topic.creation.default.partitions", "6");
        c.put("topic.creation.default.replication.factor", "1");
        c.put("key.converter", "org.apache.kafka.connect.json.JsonConverter");
        c.put("key.converter.schemas.enable", "false");
        c.put("value.converter", "org.apache.kafka.connect.json.JsonConverter");
        c.put("value.converter.schemas.enable", "false");
        return c;
    }
}
