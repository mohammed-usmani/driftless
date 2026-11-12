package dev.driftless.server.apply;

/** What applying one change did to one generation. */
public enum Outcome { EMBED, SKIP, METADATA_ONLY, DELETE, STALE, TRUNCATE, FAILED }
