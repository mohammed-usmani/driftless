package dev.driftless.server.source;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import dev.driftless.server.config.PipelineConfig;
import dev.driftless.server.support.IntegrationTest;
import dev.driftless.server.support.KafkaProbe;
import dev.driftless.server.support.Pipelines;
import dev.driftless.server.support.SourceDb;
import java.time.Duration;
import java.util.List;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.json.JsonMapper;

class ConnectorManagerIT extends IntegrationTest {

    private static final String T = "cm_articles";
    private static final JsonMapper JSON = JsonMapper.builder().build();
    private final PipelineConfig pipeline = Pipelines.articles(T);

    @Autowired
    ConnectorManager connectors;

    @Autowired
    SignalSender signals;

    @BeforeAll
    static void table() {
        SourceDb.createArticles(sourceJdbc(), T);
    }

    @Test
    void ensureIsIdempotentAndStreamsChanges() {
        connectors.ensure(pipeline);
        connectors.ensure(pipeline);
        await().atMost(Duration.ofMinutes(1)).until(() -> connectors.state(T) == ConnectorManager.State.RUNNING);
        assertThat(connectors.names()).containsOnlyOnce("driftless-" + T);
        awaitSlotActive(T);

        sourceJdbc().update("INSERT INTO " + T + " (id, title, body) VALUES (1, 'Hello', 'World')");
        try (var probe = new KafkaProbe(KAFKA.getBootstrapServers(), connectors.topic(pipeline))) {
            var record = probe.await(r -> r.value() != null && JSON.readTree(r.value()).path("op").asString().equals("c"),
                    Duration.ofSeconds(60));
            assertThat(record.key()).isEqualTo("{\"id\":1}");

            String signal = signals.executeSnapshot(pipeline, List.of("1"));
            assertThat(signal).isNotBlank();
            var read = probe.await(r -> {
                JsonNode v = r.value() == null ? null : JSON.readTree(r.value());
                return v != null && v.path("op").asString().equals("r") && v.at("/after/id").asLong() == 1;
            }, Duration.ofSeconds(60));
            assertThat(read.key()).isEqualTo("{\"id\":1}");
        }
    }
}
