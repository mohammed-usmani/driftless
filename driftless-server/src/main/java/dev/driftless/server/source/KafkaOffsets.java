package dev.driftless.server.source;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaAdmin;
import org.springframework.stereotype.Component;

/** End offsets of a change topic and how far the change consumer group has committed, by partition. */
@Component
public class KafkaOffsets {

    private final KafkaAdmin kafkaAdmin;
    private final String group;

    public KafkaOffsets(KafkaAdmin kafkaAdmin, @Value("${driftless.consumer-group:driftless}") String group) {
        this.kafkaAdmin = kafkaAdmin;
        this.group = group;
    }

    public Map<Integer, Long> end(String topic) {
        try (Admin admin = Admin.create(kafkaAdmin.getConfigurationProperties())) {
            var partitions = admin.describeTopics(List.of(topic)).allTopicNames().get().get(topic).partitions();
            Map<TopicPartition, OffsetSpec> request = new HashMap<>();
            partitions.forEach(pi -> request.put(new TopicPartition(topic, pi.partition()), OffsetSpec.latest()));
            Map<Integer, Long> out = new HashMap<>();
            admin.listOffsets(request).all().get().forEach((tp, info) -> out.put(tp.partition(), info.offset()));
            return out;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException(e);
        } catch (ExecutionException e) {
            throw new IllegalStateException("could not read end offsets of " + topic, e.getCause());
        }
    }

    public Map<Integer, Long> committed(String topic) {
        try (Admin admin = Admin.create(kafkaAdmin.getConfigurationProperties())) {
            Map<TopicPartition, OffsetAndMetadata> all = admin.listConsumerGroupOffsets(group).partitionsToOffsetAndMetadata().get();
            Map<Integer, Long> out = new HashMap<>();
            all.forEach((tp, om) -> {
                if (tp.topic().equals(topic) && om != null) {
                    out.put(tp.partition(), om.offset());
                }
            });
            return out;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException(e);
        } catch (ExecutionException e) {
            throw new IllegalStateException("could not read offsets of group " + group, e.getCause());
        }
    }

    /** Records on the topic the consumer group hasn't committed yet. */
    public long lag(String topic) {
        Map<Integer, Long> committed = committed(topic);
        return end(topic).entrySet().stream().mapToLong(e -> Math.max(0, e.getValue() - committed.getOrDefault(e.getKey(), 0L))).sum();
    }
}
