package dev.driftless.server.generation;

public record Chunk(int no, String text, String hash, float[] embedding) {
}
