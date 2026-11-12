package dev.driftless.server.support;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.function.Predicate;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

/** Reads a topic from the beginning with a throwaway consumer group. */
public final class KafkaProbe implements AutoCloseable {

    private final KafkaConsumer<String, String> consumer;
    private final List<ConsumerRecord<String, String>> seen = new ArrayList<>();

    public KafkaProbe(String bootstrap, String topic) {
        consumer = new KafkaConsumer<>(Map.of(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap,
                ConsumerConfig.GROUP_ID_CONFIG, "probe-" + UUID.randomUUID(),
                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest",
                ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false",
                // pattern subscriptions only notice new topics on a metadata refresh
                ConsumerConfig.METADATA_MAX_AGE_CONFIG, "500"),
                new StringDeserializer(), new StringDeserializer());
        consumer.subscribe(java.util.regex.Pattern.compile(java.util.regex.Pattern.quote(topic)));
    }

    /** Polls until a record matches or the timeout passes; returns every record seen so far. */
    public ConsumerRecord<String, String> await(Predicate<ConsumerRecord<String, String>> match, Duration timeout) {
        for (var r : seen) {
            if (match.test(r)) {
                return r;
            }
        }
        Instant end = Instant.now().plus(timeout);
        while (Instant.now().isBefore(end)) {
            for (var r : consumer.poll(Duration.ofMillis(500))) {
                seen.add(r);
                if (match.test(r)) {
                    return r;
                }
            }
        }
        throw new AssertionError("no matching record within " + timeout + "; saw " + seen.size());
    }

    /** Everything seen so far plus whatever arrives within {@code quiet} of the last record. */
    public List<ConsumerRecord<String, String>> drain(Duration quiet) {
        while (true) {
            var batch = consumer.poll(quiet);
            if (batch.isEmpty()) {
                return List.copyOf(seen);
            }
            batch.forEach(seen::add);
        }
    }

    @Override
    public void close() {
        consumer.close();
    }
}
