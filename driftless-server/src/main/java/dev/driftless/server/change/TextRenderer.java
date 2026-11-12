package dev.driftless.server.change;

import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/** Renders a pipeline's text template: {@code {column}} is substituted, {@code {{} and {@code }}} are literal braces. */
public final class TextRenderer {

    public static final String UNAVAILABLE = "__debezium_unavailable_value";
    private static final Pattern TOKEN = Pattern.compile("\\{\\{|}}|\\{([a-z_][a-z0-9_]*)}");

    private TextRenderer() { }

    public static String render(String template, Map<String, Object> row) {
        Matcher m = TOKEN.matcher(template);
        StringBuilder out = new StringBuilder();
        while (m.find()) {
            String replacement;
            if (m.group(1) == null) {
                replacement = m.group().equals("{{") ? "{" : "}";
            } else {
                String col = m.group(1);
                if (!row.containsKey(col)) {
                    throw new RenderException("unknown column: " + col);
                }
                Object v = row.get(col);
                if (v == null) {
                    replacement = "";
                } else if (v instanceof String s && s.contains(UNAVAILABLE)) {
                    throw new RenderException("unavailable value in column: " + col);
                } else {
                    replacement = RowValues.canonical(v);
                }
            }
            m.appendReplacement(out, Matcher.quoteReplacement(replacement));
        }
        m.appendTail(out);
        return out.toString();
    }

    /** Columns the template references, in first-use order. */
    public static List<String> columns(String template) {
        Matcher m = TOKEN.matcher(template);
        LinkedHashSet<String> cols = new LinkedHashSet<>();
        while (m.find()) {
            if (m.group(1) != null) {
                cols.add(m.group(1));
            }
        }
        return List.copyOf(cols);
    }
}
