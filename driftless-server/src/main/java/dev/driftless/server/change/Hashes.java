package dev.driftless.server.change;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HexFormat;
import java.util.Map;
import java.util.TreeMap;

public final class Hashes {

    private Hashes() { }

    public static String sha256Hex(String s) {
        try {
            return HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(s.getBytes(StandardCharsets.UTF_8)));
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException(e);
        }
    }

    /** Order-independent hash of metadata values, rendered with {@link RowValues#canonical}. */
    public static String metadata(Map<String, Object> metadata) {
        StringBuilder sb = new StringBuilder();
        new TreeMap<>(metadata).forEach((k, v) -> sb.append(k).append('\u0001').append(RowValues.canonical(v)).append('\u0002'));
        return sha256Hex(sb.toString());
    }
}
