package dev.driftless.server.generation;

/** BUILDING → READY → LIVE → DRAINING → DROPPED. Every status but DROPPED receives live changes. */
public enum GenerationStatus { BUILDING, READY, LIVE, DRAINING, DROPPED }
