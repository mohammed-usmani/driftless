package dev.driftless.server.support;

import org.springframework.jdbc.core.JdbcTemplate;

/** Creates the demo {@code articles} table with everything Debezium and Driftless need. */
public final class SourceDb {

    private SourceDb() { }

    public static void createArticles(JdbcTemplate source) {
        createArticles(source, "articles");
    }

    /** Drops and recreates {@code table}, its signal table and its publication {@code driftless_<table>}. */
    public static void createArticles(JdbcTemplate source, String table) {
        source.execute("DROP PUBLICATION IF EXISTS driftless_" + table);
        source.execute("DROP TABLE IF EXISTS " + table);
        source.execute("""
                CREATE TABLE %s (
                  id bigint PRIMARY KEY,
                  title text,
                  body text,
                  category text,
                  tenant_id text,
                  acl text[],
                  updated_at timestamptz NOT NULL DEFAULT now()
                )""".formatted(table));
        source.execute("ALTER TABLE " + table + " REPLICA IDENTITY FULL");
        source.execute("""
                CREATE TABLE IF NOT EXISTS driftless_signal (
                  id varchar(64) PRIMARY KEY,
                  type varchar(32) NOT NULL,
                  data text
                )""");
        source.execute("CREATE PUBLICATION driftless_" + table + " FOR TABLE " + table + ", driftless_signal");
    }
}
