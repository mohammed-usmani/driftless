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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.json.JsonMapper;

/**
 * Seeds the demo {@code articles} table and writes a mix of changes to it at a target rate.
 *
 * <pre>
 *   seed --file ops/demo-data/askubuntu.jsonl [--limit 5000]
 *   run  [--rate 20] [--duration 60s] [--mix insert=5,title=20,body=20,touch=35,acl=10,delete=10] [--threads 1]
 * </pre>
 * The source database comes from SOURCE_URL / SOURCE_USER / SOURCE_PASSWORD.
 */
public final class LoadGen {

    private static final JsonMapper JSON = JsonMapper.builder().build();
    private static final String[] TENANTS = {"acme", "globex", "initech"};

    private LoadGen() { }

    public static void main(String[] args) throws Exception {
        if (args.length == 0 || !List.of("seed", "run").contains(args[0])) {
            System.err.println("usage: loadgen seed --file <jsonl> [--limit N] | run [--rate R] [--duration 60s] [--mix ...] [--threads N]");
            System.exit(2);
        }
        Map<String, String> opts = new HashMap<>();
        for (int i = 1; i + 1 < args.length; i += 2) {
            opts.put(args[i].replaceFirst("^--", ""), args[i + 1]);
        }
        if (args[0].equals("seed")) {
            try (Connection c = connect()) {
                seed(c, Path.of(opts.getOrDefault("file", "ops/demo-data/askubuntu.jsonl")), Integer.parseInt(opts.getOrDefault("limit", "5000")));
            }
        } else {
            run(Double.parseDouble(opts.getOrDefault("rate", "20")), duration(opts.getOrDefault("duration", "60s")),
                    Mix.parse(opts.getOrDefault("mix", Mix.DEFAULT)), Integer.parseInt(opts.getOrDefault("threads", "1")));
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

    /**
     * Writes the mix at {@code rate} changes/s from {@code threads} connections. Thread t owns the existing rows with
     * {@code id % threads == t} and its own insert id range, so no thread updates or deletes a row another one removed.
     */
    static void run(double rate, Duration duration, Mix mix, int threads) throws Exception {
        long maxId;
        try (Connection c = connect(); ResultSet rs = c.createStatement().executeQuery("SELECT coalesce(max(id), 0) FROM articles")) {
            rs.next();
            maxId = rs.getLong(1);
        }
        long idBase = Math.max(maxId, 10_000_000L) + 1; // inserts stay clear of existing ids
        long start = System.nanoTime();
        List<Future<long[]>> results = new ArrayList<>();
        try (var pool = Executors.newFixedThreadPool(threads)) {
            for (int t = 0; t < threads; t++) {
                int slice = t;
                results.add(pool.submit(() -> {
                    try (Connection c = connect()) {
                        return run(c, rate / threads, duration, mix, idBase + slice * 100_000_000L, slice, threads);
                    }
                }));
            }
        }
        long ops = 0;
        long missed = 0;
        for (var r : results) {
            ops += r.get()[0];
            missed += r.get()[1];
        }
        double seconds = (System.nanoTime() - start) / 1e9;
        System.out.printf("%d ops in %.1fs: %.1f ops/s (%d hit no row)%n", ops, seconds, ops / seconds, missed);
    }

    /** One writer: returns {ops, ops that changed no row}. */
    static long[] run(Connection c, double rate, Duration duration, Mix mix, long nextId, int slice, int slices) throws SQLException {
        c.setAutoCommit(true);
        List<Long> ids = new ArrayList<>(); // this writer's rows that exist, so updates and deletes always hit one
        try (PreparedStatement ps = c.prepareStatement("SELECT id FROM articles WHERE id % ? = ?")) {
            ps.setLong(1, slices);
            ps.setLong(2, slice);
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    ids.add(rs.getLong(1));
                }
            }
        }
        Random rnd = new Random();
        long start = System.nanoTime();
        long end = start + duration.toNanos();
        long intervalNanos = (long) (1_000_000_000L / rate);
        long ops = 0;
        long missed = 0;
        while (System.nanoTime() < end) {
            Mix.Op op = ids.isEmpty() ? Mix.Op.INSERT : mix.draw(rnd);
            int at = rnd.nextInt(Math.max(1, ids.size()));
            long key = op == Mix.Op.INSERT ? nextId++ : ids.get(at);
            if (apply(c, op, key, rnd) == 0) {
                missed++; // deleted by something outside the load generator
            }
            if (op == Mix.Op.INSERT) {
                ids.add(key);
            } else if (op == Mix.Op.DELETE) {
                ids.set(at, ids.getLast());
                ids.removeLast();
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
        return new long[] {ops, missed};
    }

    private static Connection connect() throws SQLException {
        return DriverManager.getConnection(
                env("SOURCE_URL", "jdbc:postgresql://localhost:5443/source"), env("SOURCE_USER", "app"), env("SOURCE_PASSWORD", "app"));
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
