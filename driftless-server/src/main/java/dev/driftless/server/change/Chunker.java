package dev.driftless.server.change;

import java.util.ArrayList;
import java.util.List;

/**
 * Splits text into chunks of at most {@code maxChars}: on paragraphs first, then lines, then sentences,
 * then words, and as a last resort at fixed offsets. Each chunk after the first starts with up to
 * {@code overlapChars} of whole pieces carried over from the end of the previous one.
 */
public final class Chunker {

    private static final String[] SEPARATORS = {"\n\n", "\n", ". ", " "};

    private Chunker() { }

    public static List<String> split(String text, int maxChars, int overlapChars) {
        if (text.isEmpty()) {
            return List.of();
        }
        int overlap = Math.min(overlapChars, maxChars / 2);
        List<String> pieces = new ArrayList<>();
        atoms(text, maxChars, 0, pieces);

        List<String> chunks = new ArrayList<>();
        List<String> current = new ArrayList<>();
        int length = 0;
        for (String piece : pieces) {
            if (length + piece.length() > maxChars && !current.isEmpty()) {
                chunks.add(String.join("", current));
                List<String> carried = tail(current, overlap);
                int carriedLength = carried.stream().mapToInt(String::length).sum();
                if (carriedLength + piece.length() > maxChars) {
                    carried = List.of();
                    carriedLength = 0;
                }
                current = new ArrayList<>(carried);
                length = carriedLength;
            }
            current.add(piece);
            length += piece.length();
        }
        chunks.add(String.join("", current));
        return chunks;
    }

    /** Breaks {@code s} into pieces of at most {@code max} chars; joining them gives back {@code s}. */
    private static void atoms(String s, int max, int level, List<String> out) {
        if (s.length() <= max) {
            out.add(s);
            return;
        }
        if (level == SEPARATORS.length) {
            for (int i = 0; i < s.length(); i += max) {
                out.add(s.substring(i, Math.min(s.length(), i + max)));
            }
            return;
        }
        String sep = SEPARATORS[level];
        int from = 0;
        while (from < s.length()) {
            int at = s.indexOf(sep, from);
            int end = at < 0 ? s.length() : at + sep.length();
            String part = s.substring(from, end);
            if (part.length() <= max) {
                out.add(part);
            } else {
                atoms(part, max, level + 1, out);
            }
            from = end;
        }
    }

    /** The longest run of whole pieces at the end of {@code pieces} that fits in {@code budget}. */
    private static List<String> tail(List<String> pieces, int budget) {
        int used = 0;
        int i = pieces.size();
        while (i > 0 && used + pieces.get(i - 1).length() <= budget) {
            used += pieces.get(i - 1).length();
            i--;
        }
        return i == pieces.size() ? List.of() : pieces.subList(i, pieces.size());
    }
}
