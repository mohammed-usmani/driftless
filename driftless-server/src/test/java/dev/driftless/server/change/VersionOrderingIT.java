package dev.driftless.server.change;

import static org.assertj.core.api.Assertions.assertThat;

import dev.driftless.server.config.PipelineConfig;
import dev.driftless.server.source.ConnectorManager;
import dev.driftless.server.source.SignalSender;
import dev.driftless.server.support.IntegrationTest;
import dev.driftless.server.support.KafkaProbe;
import dev.driftless.server.support.Pipelines;
import dev.driftless.server.support.SourceDb;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.json.JsonMapper;

/**
 * The contract for {@link Version}: replaying every record of a key in version order must end in the
 * source's state, under concurrent multi-statement transactions, TOASTed bodies and incremental snapshots.
 */
class VersionOrderingIT extends IntegrationTest {

    private static final String T = "vo_articles";
    private static final int KEYS = 50;
    private static final JsonMapper JSON = JsonMapper.builder().build();

    @Autowired ConnectorManager connectors;
    @Autowired SignalSender signals;

    record Rec(long offset, int partition, String op, long key, Version version, JsonNode value) { }

    @Test
    void maxVersionPerKeyIsTheSourceStateAndVersionsFollowStreamOrder() throws Exception {
        PipelineConfig p = Pipelines.articles(T);
        SourceDb.createArticles(sourceJdbc(), T);
        connectors.ensure(p);
        Awaitility.await().atMost(Duration.ofMinutes(1)).until(() -> connectors.state(T) == ConnectorManager.State.RUNNING);
        awaitSlotActive(T);

        try (KafkaProbe probe = new KafkaProbe(KAFKA.getBootstrapServers(), connectors.topic(p))) {
            Instant end = Instant.now().plusSeconds(20);
            List<Thread> writers = new ArrayList<>();
            for (int i = 0; i < 4; i++) {
                writers.add(Thread.ofPlatform().start(() -> write(end)));
            }
            Thread.sleep(5_000);
            signals.executeSnapshot(p, List.of());
            Thread.sleep(3_000);
            connectors.restart(T); // versions must stay ordered across a connector restart
            Thread.sleep(4_000);
            signals.executeSnapshot(p, List.of());
            for (Thread w : writers) {
                w.join();
            }

            List<Rec> recs = probe.drain(Duration.ofSeconds(15)).stream()
                    .filter(r -> r.value() != null).map(VersionOrderingIT::rec).toList();
            saveSamples(recs);
            assertThat(recs.stream().filter(r -> r.op().equals("r"))).isNotEmpty();

            // per key, in stream order: streamed changes strictly increase; snapshot reads never go backwards
            Map<Long, Rec> last = new HashMap<>();
            List<String> violations = new ArrayList<>();
            recs.stream().sorted((a, b) -> Long.compare(a.offset(), b.offset())).forEach(r -> {
                Rec prev = last.get(r.key());
                if (prev != null) {
                    int c = r.version().compareTo(prev.version());
                    if (r.op().equals("r") ? c < 0 : c <= 0) {
                        violations.add("key " + r.key() + ": " + prev.op() + "@" + prev.version() + " then " + r.op() + "@" + r.version());
                    }
                }
                last.put(r.key(), r);
            });
            assertThat(violations).as("version order violations").isEmpty();

            // the max-version record of every key is the source's final state
            Map<Long, Rec> max = new HashMap<>();
            for (Rec r : recs) {
                max.merge(r.key(), r, (a, b) -> a.version().compareTo(b.version()) >= 0 ? a : b);
            }
            List<String> wrong = new ArrayList<>();
            for (long k = 1; k <= KEYS; k++) {
                List<Map<String, Object>> rows = sourceJdbc().queryForList("SELECT title, body FROM " + T + " WHERE id = ?", k);
                Rec r = max.get(k);
                if (rows.isEmpty()) {
                    if (r != null && !r.op().equals("d")) {
                        wrong.add(k + ": deleted in source but max record is " + r.op());
                    }
                } else if (r == null || r.op().equals("d")) {
                    wrong.add(k + ": exists in source but max record is " + (r == null ? "missing" : "d"));
                } else {
                    String title = (String) rows.getFirst().get("title");
                    String body = (String) rows.getFirst().get("body");
                    if (!title.equals(r.value().at("/after/title").asString())
                            || !sha(body).equals(sha(r.value().at("/after/body").asString()))) {
                        wrong.add(k + ": max record " + r.op() + "@" + r.version() + " has stale or unavailable values");
                    }
                }
            }
            assertThat(wrong).as("keys whose max-version record is not the source state").isEmpty();
        }
    }

    private static Rec rec(ConsumerRecord<String, String> r) {
        JsonNode v = JSON.readTree(r.value());
        String op = v.path("op").asString();
        JsonNode row = op.equals("d") ? v.path("before") : v.path("after");
        return new Rec(r.offset(), r.partition(), op, row.path("id").asLong(), Version.fromSource(v.path("source"), op), v);
    }

    private void write(Instant end) {
        try (Connection c = DriverManager.getConnection(sourceJdbcUrl(), POSTGRES.getUsername(), POSTGRES.getPassword())) {
            c.setAutoCommit(false);
            ThreadLocalRandom rnd = ThreadLocalRandom.current();
            while (Instant.now().isBefore(end)) {
                try (var s = c.createStatement()) {
                    int statements = 1 + rnd.nextInt(3);
                    for (int i = 0; i < statements; i++) {
                        long k = 1 + rnd.nextInt(KEYS);
                        String title = "t" + rnd.nextInt(1_000_000);
                        switch (rnd.nextInt(5)) {
                            case 0, 1 -> s.executeUpdate("INSERT INTO " + T + " (id, title, body) VALUES (" + k + ", '" + title
                                    + "', '" + body(rnd) + "') ON CONFLICT (id) DO UPDATE SET title = excluded.title, body = excluded.body");
                            case 2 -> s.executeUpdate("UPDATE " + T + " SET title = '" + title + "' WHERE id = " + k);
                            case 3 -> s.executeUpdate("UPDATE " + T + " SET body = '" + body(rnd) + "' WHERE id = " + k);
                            default -> s.executeUpdate("DELETE FROM " + T + " WHERE id = " + k);
                        }
                        Thread.sleep(rnd.nextInt(3));
                    }
                    c.commit();
                } catch (SQLException e) {
                    c.rollback(); // deadlocks between writers are expected; the transaction simply didn't happen
                }
            }
        } catch (SQLException | InterruptedException e) {
            throw new IllegalStateException(e);
        }
    }

    /** ~10 KB of incompressible text, so Postgres stores it out of line (TOAST). */
    private static String body(ThreadLocalRandom rnd) {
        StringBuilder sb = new StringBuilder(10_000);
        for (int i = 0; i < 10_000; i++) {
            sb.append((char) ('a' + rnd.nextInt(26)));
        }
        return sb.toString();
    }

    private static String sha(String s) {
        try {
            return s == null ? "null" : HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(s.getBytes(StandardCharsets.UTF_8)));
        } catch (java.security.NoSuchAlgorithmException e) {
            throw new IllegalStateException(e);
        }
    }

    /** Keeps a few real records of each op (body shortened) as fixtures for the decoder tests. */
    private static void saveSamples(List<Rec> recs) throws java.io.IOException {
        Path dir = Path.of("target/debezium-samples");
        Files.createDirectories(dir);
        Map<String, Integer> count = new HashMap<>();
        for (Rec r : recs) {
            int n = count.merge(r.op(), 1, Integer::sum);
            if (n <= 3) {
                Files.writeString(dir.resolve(r.op() + "-" + n + ".json"), r.value().toPrettyString());
            }
        }
    }
}
