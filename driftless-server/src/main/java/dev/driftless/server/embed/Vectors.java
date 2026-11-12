package dev.driftless.server.embed;

/** pgvector's text form, {@code [0.1,0.2]}. Float.toString round-trips exactly. */
public final class Vectors {

    private Vectors() { }

    public static String toText(float[] v) {
        StringBuilder sb = new StringBuilder(v.length * 12).append('[');
        for (int i = 0; i < v.length; i++) {
            if (i > 0) {
                sb.append(',');
            }
            sb.append(v[i]);
        }
        return sb.append(']').toString();
    }

    public static float[] parse(String text) {
        String body = text.substring(1, text.length() - 1);
        if (body.isEmpty()) {
            return new float[0];
        }
        String[] parts = body.split(",");
        float[] v = new float[parts.length];
        for (int i = 0; i < parts.length; i++) {
            v[i] = Float.parseFloat(parts[i]);
        }
        return v;
    }
}
