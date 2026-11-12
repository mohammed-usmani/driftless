package dev.driftless.server.support;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.atomic.AtomicLong;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Concurrent writers doing random multi-statement transactions on a small set of hot keys of an
 * {@code articles}-shaped table: inserts, title-only and body updates, touch-only updates, ACL changes, deletes.
 */
public final class RandomWriters {

    private static final Logger log = LoggerFactory.getLogger(RandomWriters.class);

    private RandomWriters() { }

    public record Stats(long commits, long rollbacks) { }

    public static Stats run(String jdbcUrl, String user, String password, String table, int writers, Duration duration,
                            int hotKeys, long seed) {
        log.info("random writers on {}: {} writers, {}, {} keys, seed {}", table, writers, duration, hotKeys, seed);
        Instant end = Instant.now().plus(duration);
        AtomicLong commits = new AtomicLong();
        AtomicLong rollbacks = new AtomicLong();
        List<Thread> threads = new ArrayList<>();
        for (int w = 0; w < writers; w++) {
            long s = seed * 31 + w;
            threads.add(Thread.ofPlatform().start(() -> write(jdbcUrl, user, password, table, end, hotKeys, new Random(s), commits, rollbacks)));
        }
        for (Thread t : threads) {
            try {
                t.join();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IllegalStateException(e);
            }
        }
        return new Stats(commits.get(), rollbacks.get());
    }

    private static void write(String url, String user, String password, String table, Instant end, int hotKeys, Random rnd,
                              AtomicLong commits, AtomicLong rollbacks) {
        try (Connection c = DriverManager.getConnection(url, user, password)) {
            c.setAutoCommit(false);
            while (Instant.now().isBefore(end)) {
                try {
                    int statements = 1 + rnd.nextInt(3);
                    for (int i = 0; i < statements; i++) {
                        statement(c, table, 1 + rnd.nextInt(hotKeys), rnd);
                    }
                    c.commit();
                    commits.incrementAndGet();
                } catch (SQLException e) {
                    c.rollback(); // deadlocks between writers are expected
                    rollbacks.incrementAndGet();
                }
            }
        } catch (SQLException e) {
            throw new IllegalStateException(e);
        }
    }

    private static void statement(Connection c, String table, long key, Random rnd) throws SQLException {
        int op = rnd.nextInt(100);
        String sql;
        if (op < 25) {
            sql = "INSERT INTO " + table + " (id, title, body, category, tenant_id, acl) VALUES (?, ?, ?, ?, ?, ?) "
                    + "ON CONFLICT (id) DO UPDATE SET title = excluded.title, body = excluded.body";
        } else if (op < 45) {
            sql = "UPDATE " + table + " SET title = ? WHERE id = ?";
        } else if (op < 60) {
            sql = "UPDATE " + table + " SET body = ? WHERE id = ?";
        } else if (op < 75) {
            sql = "UPDATE " + table + " SET updated_at = now() WHERE id = ?";
        } else if (op < 88) {
            sql = "UPDATE " + table + " SET acl = ? WHERE id = ?";
        } else {
            sql = "DELETE FROM " + table + " WHERE id = ?";
        }
        try (PreparedStatement ps = c.prepareStatement(sql)) {
            if (op < 25) {
                ps.setLong(1, key);
                ps.setString(2, title(rnd));
                ps.setString(3, body(rnd));
                ps.setString(4, "cat-" + rnd.nextInt(5));
                ps.setString(5, "t-" + rnd.nextInt(3));
                ps.setArray(6, c.createArrayOf("text", acl(rnd)));
            } else if (op < 45) {
                ps.setString(1, title(rnd));
                ps.setLong(2, key);
            } else if (op < 60) {
                ps.setString(1, body(rnd));
                ps.setLong(2, key);
            } else if (op < 75) {
                ps.setLong(1, key);
            } else if (op < 88) {
                ps.setArray(1, c.createArrayOf("text", acl(rnd)));
                ps.setLong(2, key);
            } else {
                ps.setLong(1, key);
            }
            ps.executeUpdate();
        }
    }

    private static String title(Random rnd) {
        return "Title " + Long.toString(rnd.nextLong() & Long.MAX_VALUE, 36);
    }

    /** 100 to 10,000 chars of words, sentences and paragraphs, so rows have different chunk counts. */
    static String body(Random rnd) {
        int length = 100 + rnd.nextInt(9_900);
        StringBuilder sb = new StringBuilder(length + 16);
        while (sb.length() < length) {
            sb.append(Long.toString(rnd.nextLong() & 0xFFFFFFFL, 36));
            int r = rnd.nextInt(40);
            sb.append(r == 0 ? "\n\n" : r < 4 ? ". " : " ");
        }
        return sb.toString();
    }

    private static String[] acl(Random rnd) {
        return rnd.nextBoolean() ? new String[] {"u" + rnd.nextInt(4)} : new String[] {"u" + rnd.nextInt(4), "admins"};
    }
}
