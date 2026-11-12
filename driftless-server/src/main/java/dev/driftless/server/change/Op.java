package dev.driftless.server.change;

/** What a change does to the index. Debezium's c, u and r are all upserts. */
public enum Op { UPSERT, DELETE, TRUNCATE }
