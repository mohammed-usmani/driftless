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
        create(source, table, "bigint", "");
    }

    /** An articles-shaped table with the given primary key type and any extra columns (", price numeric(10,2)"). */
    public static void create(JdbcTemplate source, String table, String idType, String extraColumns) {
        source.execute("DROP PUBLICATION IF EXISTS driftless_" + table);
        source.execute("DROP TABLE IF EXISTS " + table);
        source.execute("""
                CREATE TABLE %s (
                  id %s PRIMARY KEY,
                  title text,
                  body text,
                  category text,
                  tenant_id text,
                  acl text[],
                  updated_at timestamptz NOT NULL DEFAULT now()%s
                )""".formatted(table, idType, extraColumns));
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
