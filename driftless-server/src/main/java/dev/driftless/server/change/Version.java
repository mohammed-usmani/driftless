package dev.driftless.server.change;

import tools.jackson.databind.JsonNode;

/** Position of a change in source commit order. See docs/adr/0001-row-versions.md. */
public record Version(long major, long minor) implements Comparable<Version> {

    public static Version fromSource(JsonNode source, String op) {
        // sequence is a JSON array encoded as a string: ["lastCommitLsn","lsn"], either may be null
        JsonNode seq = JSON.readTree(source.path("sequence").asString("[null,null]"));
        return new Version(lsn(seq.get(0)), lsn(seq.get(1)));
    }

    private static long lsn(JsonNode n) {
        return n == null || n.isNull() ? 0 : Long.parseLong(n.asString());
    }

    private static final tools.jackson.databind.json.JsonMapper JSON = tools.jackson.databind.json.JsonMapper.builder().build();

    /** The smallest version greater than this one that no real change can carry. */
    public Version justAfter() {
        return new Version(major, minor + 1);
    }

    @Override
    public int compareTo(Version o) {
        int c = Long.compare(major, o.major);
        return c != 0 ? c : Long.compare(minor, o.minor);
    }

    @Override
    public String toString() {
        return major + "/" + minor;
    }
}
