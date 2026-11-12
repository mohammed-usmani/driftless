package dev.driftless.server.source;

import dev.driftless.server.config.DriftlessProperties;
import java.util.List;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.json.JsonMapper;

/** Debezium's notification topic: marks the running snapshot done when it reports COMPLETED or ABORTED. */
@Component
public class SnapshotNotificationListener {

    private static final Logger log = LoggerFactory.getLogger(SnapshotNotificationListener.class);
    private static final JsonMapper JSON = JsonMapper.builder().build();

    private final DriftlessProperties props;
    private final SnapshotQueue queue;

    public SnapshotNotificationListener(DriftlessProperties props, SnapshotQueue queue) {
        this.props = props;
        this.queue = queue;
    }

    @KafkaListener(id = "notifications", groupId = "${driftless.consumer-group:driftless}-notifications",
            topicPattern = "driftless\\.[a-z0-9_]+\\.notifications")
    public void onBatch(List<ConsumerRecord<byte[], byte[]>> records, Acknowledgment ack) {
        for (ConsumerRecord<byte[], byte[]> r : records) {
            String pipeline = r.topic().split("\\.")[1];
            if (props.pipeline(pipeline) == null || r.value() == null) {
                continue;
            }
            JsonNode n = JSON.readTree(r.value());
            String aggregate = n.path("aggregate_type").asString(n.path("aggregateType").asString(""));
            String type = n.path("type").asString("");
            log.debug("notification for {}: {} {}", pipeline, aggregate, type);
            if (aggregate.equals("Incremental Snapshot") && (type.equals("COMPLETED") || type.equals("ABORTED"))) {
                queue.finish(pipeline, type.equals("COMPLETED"))
                        .ifPresent(f -> log.info("snapshot request {} for {} {}", f.requestId(), pipeline, type.toLowerCase()));
            }
        }
        ack.acknowledge();
    }
}
