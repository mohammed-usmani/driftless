package dev.driftless.server.change;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.HashMap;
import java.util.Map;
import org.junit.jupiter.api.Test;

class TextRendererTest {

    @Test
    void substitutesColumns() {
        assertThat(TextRenderer.render("{title}\n\n{body}", Map.of("title", "Hi", "body", "There"))).isEqualTo("Hi\n\nThere");
    }

    @Test
    void nullColumnRendersEmpty() {
        Map<String, Object> row = new HashMap<>();
        row.put("title", "Hi");
        row.put("body", null);
        assertThat(TextRenderer.render("{title}|{body}", row)).isEqualTo("Hi|");
    }

    @Test
    void doubledBracesAreLiteral() {
        assertThat(TextRenderer.render("{{x}} {title}", Map.of("title", "Hi"))).isEqualTo("{x} Hi");
    }

    @Test
    void unknownColumnThrows() {
        assertThatThrownBy(() -> TextRenderer.render("{nope}", Map.of("title", "Hi")))
                .isInstanceOf(RenderException.class).hasMessage("unknown column: nope");
    }

    @Test
    void unavailablePlaceholderThrows() {
        assertThatThrownBy(() -> TextRenderer.render("{body}", Map.of("body", "__debezium_unavailable_value")))
                .isInstanceOf(RenderException.class).hasMessage("unavailable value in column: body");
    }

    @Test
    void columnsReferencedAreListed() {
        assertThat(TextRenderer.columns("{{x}} {title}\n{body}{title}")).containsExactly("title", "body");
    }
}
