package dev.driftless.loadgen;

import java.io.BufferedReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.json.JsonMapper;

/**
 * Seeds the demo {@code articles} table and writes a mix of changes to it at a target rate.
 *
 * <pre>
 *   seed --file ops/demo-data/askubuntu.jsonl [--limit 5000]
 *   run  [--rate 20] [--duration 60s] [--mix insert=5,title=20,body=20,touch=35,acl=10,delete=10]
 * </pre>
 * The source database comes from SOURCE_URL / SOURCE_USER / SOURCE_PASSWORD.
 */
public final class LoadGen {

    private static final JsonMapper JSON = JsonMapper.builder().build();
    private static final String[] TENANTS = {"acme", "globex", "initech"};

    private LoadGen() { }

    public static void main(String[] args) throws Exception {
        if (args.length == 0 || !List.of("seed", "run").contains(args[0])) {
            System.err.println("usage: loadgen seed --file <jsonl> [--limit N] | run [--rate R] [--duration 60s] [--mix ...]");
            System.exit(2);
        }
        Map<String, String> opts = new HashMap<>();
        for (int i = 1; i + 1 < args.length; i += 2) {
            opts.put(args[i].replaceFirst("^--", ""), args[i + 1]);
        }
        try (Connection c = DriverManager.getConnection(
                env("SOURCE_URL", "jdbc:postgresql://localhost:5443/source"), env("SOURCE_USER", "app"), env("SOURCE_PASSWORD", "app"))) {
            if (args[0].equals("seed")) {
                seed(c, Path.of(opts.getOrDefault("file", "ops/demo-data/askubuntu.jsonl")), Integer.parseInt(opts.getOrDefault("limit", "5000")));
            } else {
                run(c, Double.parseDouble(opts.getOrDefault("rate", "20")), duration(opts.getOrDefault("duration", "60s")),
                        Mix.parse(opts.getOrDefault("mix", Mix.DEFAULT)));
            }
        }
    }

    static void seed(Connection c, Path file, int limit) throws Exception {
        c.setAutoCommit(false);
        int n = 0;
        try (BufferedReader in = Files.newBufferedReader(file, StandardCharsets.UTF_8);
             PreparedStatement ps = c.prepareStatement("""
                     INSERT INTO articles (id, title, body, category, tenant_id, acl) VALUES (?, ?, ?, ?, ?, ?)
                     ON CONFLICT (id) DO UPDATE SET title = excluded.title, body = excluded.body, category = excluded.category""")) {
            String line;
            while (n < limit && (line = in.readLine()) != null) {
                JsonNode row = JSON.readTree(line);
                long id = row.get("id").asLong();
                ps.setLong(1, id);
                ps.setString(2, row.get("title").asString());
                ps.setString(3, row.get("body").asString());
                ps.setString(4, row.path("tags").path(0).asString("general"));
                ps.setString(5, TENANTS[(int) (id % TENANTS.length)]);
                ps.setArray(6, c.createArrayOf("text", id % 10 == 0 ? new String[] {"staff"} : new String[] {"public", "staff"}));
                ps.addBatch();
                if (++n % 500 == 0) {
                    ps.executeBatch();
                    c.commit();
                }
            }
            ps.executeBatch();
            c.commit();
        }
        System.out.printf("seeded %d articles%n", n);
    }

    static void run(Connection c, double rate, Duration duration, Mix mix) throws SQLException {
        c.setAutoCommit(true);
        long maxId;
        try (ResultSet rs = c.createStatement().executeQuery("SELECT coalesce(max(id), 0) FROM articles")) {
            rs.next();
            maxId = rs.getLong(1);
        }
        long nextId = Math.max(maxId, 10_000_000L) + 1; // inserts stay clear of seeded ids
        Random rnd = new Random();
        long start = System.nanoTime();
        long end = start + duration.toNanos();
        long intervalNanos = (long) (1_000_000_000L / rate);
        long ops = 0;
        long missed = 0;
        while (System.nanoTime() < end) {
            Mix.Op op = mix.draw(rnd);
            long key = op == Mix.Op.INSERT ? nextId++ : 1 + (long) (rnd.nextDouble() * Math.max(1, nextId - 1));
            if (apply(c, op, key, rnd) == 0 && op != Mix.Op.INSERT) {
                missed++; // that id was deleted or never existed
            }
            ops++;
            long due = start + ops * intervalNanos;
            long wait = due - System.nanoTime();
            if (wait > 0) {
                try {
                    Thread.sleep(wait / 1_000_000, (int) (wait % 1_000_000));
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    break;
                }
            }
        }
        double seconds = (System.nanoTime() - start) / 1e9;
        System.out.printf("%d ops in %.1fs: %.1f ops/s (%d hit no row)%n", ops, seconds, ops / seconds, missed);
    }

    private static int apply(Connection c, Mix.Op op, long key, Random rnd) throws SQLException {
        String sql = switch (op) {
            case INSERT -> "INSERT INTO articles (id, title, body, category, tenant_id, acl) VALUES (?, ?, ?, 'loadgen', 'acme', '{public}')";
            case TITLE -> "UPDATE articles SET title = ?, updated_at = now() WHERE id = ?";
            case BODY -> "UPDATE articles SET body = body || ?, updated_at = now() WHERE id = ?";
            case TOUCH -> "UPDATE articles SET updated_at = now() WHERE id = ?";
            case ACL -> "UPDATE articles SET acl = ? WHERE id = ?";
            case DELETE -> "DELETE FROM articles WHERE id = ?";
        };
        // pick an existing row for updates: nearest id at or above the random one
        if (op != Mix.Op.INSERT) {
            try (PreparedStatement find = c.prepareStatement("SELECT id FROM articles WHERE id >= ? ORDER BY id LIMIT 1")) {
                find.setLong(1, key);
                try (ResultSet rs = find.executeQuery()) {
                    if (!rs.next()) {
                        return 0;
                    }
                    key = rs.getLong(1);
                }
            }
        }
        try (PreparedStatement ps = c.prepareStatement(sql)) {
            switch (op) {
                case INSERT -> {
                    ps.setLong(1, key);
                    ps.setString(2, "Load test article " + key);
                    ps.setString(3, "Generated body " + Long.toString(rnd.nextLong() & Long.MAX_VALUE, 36));
                }
                case TITLE -> {
                    ps.setString(1, "Edited title " + Long.toString(rnd.nextLong() & Long.MAX_VALUE, 36));
                    ps.setLong(2, key);
                }
                case BODY -> {
                    ps.setString(1, "\n\nUpdate: " + Long.toString(rnd.nextLong() & Long.MAX_VALUE, 36));
                    ps.setLong(2, key);
                }
                case ACL -> {
                    ps.setArray(1, c.createArrayOf("text", rnd.nextBoolean() ? new String[] {"staff"} : new String[] {"public", "staff"}));
                    ps.setLong(2, key);
                }
                case TOUCH, DELETE -> ps.setLong(1, key);
            }
            return ps.executeUpdate();
        }
    }

    static Duration duration(String s) {
        if (s.endsWith("ms")) {
            return Duration.ofMillis(Long.parseLong(s.substring(0, s.length() - 2)));
        }
        long n = Long.parseLong(s.substring(0, s.length() - 1));
        return switch (s.charAt(s.length() - 1)) {
            case 's' -> Duration.ofSeconds(n);
            case 'm' -> Duration.ofMinutes(n);
            case 'h' -> Duration.ofHours(n);
            default -> Duration.ofSeconds(Long.parseLong(s));
        };
    }

    private static String env(String name, String fallback) {
        String v = System.getenv(name);
        return v == null || v.isBlank() ? fallback : v;
    }
}
