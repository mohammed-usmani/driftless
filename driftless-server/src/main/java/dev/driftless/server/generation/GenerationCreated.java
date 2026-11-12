package dev.driftless.server.generation;

/** Published when a generation needs filling from the source (first boot or a migration). */
public record GenerationCreated(Generation generation, boolean migration) {
}
