package dev.driftless.server.change;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import java.util.Random;
import org.junit.jupiter.api.Test;

class ChunkerTest {

    @Test
    void emptyGivesNoChunks() {
        assertThat(Chunker.split("", 2000, 200)).isEmpty();
    }

    @Test
    void shortTextIsOneChunk() {
        assertThat(Chunker.split("Hello.", 2000, 200)).containsExactly("Hello.");
    }

    @Test
    void splitsOnParagraphsFirst() {
        String a = "a".repeat(60) + ". " + "b".repeat(30);
        String b = "c".repeat(80);
        List<String> chunks = Chunker.split(a + "\n\n" + b, 100, 0);
        assertThat(chunks).containsExactly(a + "\n\n", b);
    }

    @Test
    void respectsMaxCharsAndCoversTheText() {
        Random rnd = new Random(42);
        for (int i = 0; i < 200; i++) {
            String text = randomText(rnd, 50 + rnd.nextInt(20_000));
            List<String> chunks = Chunker.split(text, 500, 50);
            assertThat(chunks).allSatisfy(c -> assertThat(c.length()).isBetween(1, 500));
            assertCoversWithBoundedOverlap(text, chunks, 50);
        }
    }

    @Test
    void overlapIsBoundedAndPresent() {
        String text = randomText(new Random(7), 5_000);
        List<String> chunks = Chunker.split(text, 400, 80);
        assertCoversWithBoundedOverlap(text, chunks, 80);
        // with sentence- and word-sized pieces there is always something to carry over
        int overlapping = 0;
        for (int i = 1; i < chunks.size(); i++) {
            String prev = chunks.get(i - 1);
            if (chunks.get(i).startsWith(prev.substring(Math.max(0, prev.length() - 1)))) {
                overlapping++;
            }
        }
        assertThat(overlapping).isPositive();
    }

    @Test
    void hardSplitsUnbrokenText() {
        String text = "x".repeat(1_000_000);
        long start = System.nanoTime();
        List<String> chunks = Chunker.split(text, 2000, 200);
        assertThat((System.nanoTime() - start) / 1_000_000).isLessThan(2000);
        assertThat(chunks).hasSizeGreaterThanOrEqualTo(500).allSatisfy(c -> assertThat(c.length()).isLessThanOrEqualTo(2000));
    }

    /** Chunks appear in order, leave no gap, and each starts at most {@code overlap} chars before the previous one ends. */
    private static void assertCoversWithBoundedOverlap(String text, List<String> chunks, int overlap) {
        int prevEnd = 0;
        for (String c : chunks) {
            int start = text.indexOf(c, Math.max(0, prevEnd - overlap));
            assertThat(start).as("chunk found in order").isGreaterThanOrEqualTo(0);
            assertThat(start).as("no gap").isLessThanOrEqualTo(prevEnd);
            assertThat(prevEnd - start).as("overlap bounded").isLessThanOrEqualTo(overlap);
            prevEnd = start + c.length();
        }
        assertThat(prevEnd).as("covers the whole text").isEqualTo(text.length());
    }

    private static String randomText(Random rnd, int length) {
        StringBuilder sb = new StringBuilder();
        while (sb.length() < length) {
            int word = 1 + rnd.nextInt(12);
            for (int i = 0; i < word; i++) {
                sb.append((char) ('a' + rnd.nextInt(26)));
            }
            int r = rnd.nextInt(100);
            sb.append(r < 3 ? "\n\n" : r < 8 ? ". " : r < 10 ? "\n" : " ");
        }
        return sb.toString();
    }
}
