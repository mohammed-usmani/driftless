package dev.driftless.server.apply;

import dev.driftless.server.change.ChangeEvent;
import dev.driftless.server.change.DebeziumDecoder;
import dev.driftless.server.change.DecodeException;
import dev.driftless.server.config.DriftlessProperties;
import dev.driftless.server.config.PipelineConfig;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

/**
 * Reads every pipeline's change topic ({@code driftless.<pipeline>.<schema>.<table>}). Offsets are committed
 * only after the whole batch is applied; any exception redelivers the batch (see KafkaConfig), so nothing is
 * skipped. It subscribes to the topics of the pipelines it runs, by name: with a pattern, an instance from before a
 * pipeline was added would be assigned that pipeline's partitions during a rolling deploy and acknowledge them unread.
 */
@Component
public class ChangeConsumer {

    private static final Logger log = LoggerFactory.getLogger(ChangeConsumer.class);

    private final DriftlessProperties props;
    private final DebeziumDecoder decoder;
    private final ChangeProcessor processor;
    private final FailedChangeRepository failed;

    public ChangeConsumer(DriftlessProperties props, DebeziumDecoder decoder, ChangeProcessor processor,
                          FailedChangeRepository failed) {
        this.props = props;
        this.decoder = decoder;
        this.processor = processor;
        this.failed = failed;
    }

    @KafkaListener(id = "changes", groupId = "${driftless.consumer-group:driftless}",
            topics = "#{@connectorManager.changeTopics()}",
            concurrency = "${driftless.consumer-concurrency:3}")
    public void onBatch(List<ConsumerRecord<byte[], byte[]>> records, Acknowledgment ack) {
        Map<String, List<ChangeEvent>> byPipeline = new LinkedHashMap<>();
        for (ConsumerRecord<byte[], byte[]> r : records) {
            String pipeline = r.topic().split("\\.")[1];
            PipelineConfig p = props.pipeline(pipeline);
            if (p == null) {
                continue;
            }
            try {
                decoder.decode(pipeline, p.key(), r.key(), r.value())
                        .ifPresent(e -> byPipeline.computeIfAbsent(pipeline, k -> new ArrayList<>()).add(e));
            } catch (DecodeException e) {
                log.warn("undecodable record on {} at offset {}: {}", r.topic(), r.offset(), e.getMessage());
                failed.recordUndecodable(pipeline, e.getMessage(),
                        r.value() == null ? null : new String(r.value(), StandardCharsets.UTF_8));
            }
        }
        byPipeline.forEach(processor::process);
        ack.acknowledge();
    }
}
