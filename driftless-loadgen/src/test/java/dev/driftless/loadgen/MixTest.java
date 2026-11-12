package dev.driftless.loadgen;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.EnumMap;
import java.util.Map;
import java.util.Random;
import org.junit.jupiter.api.Test;

class MixTest {

    @Test
    void parsesWeights() {
        Mix mix = Mix.parse("insert=5,title=20,body=20,touch=35,acl=10,delete=10");
        assertThat(mix.weights()).containsExactly(Map.entry(Mix.Op.INSERT, 5), Map.entry(Mix.Op.TITLE, 20), Map.entry(Mix.Op.BODY, 20),
                Map.entry(Mix.Op.TOUCH, 35), Map.entry(Mix.Op.ACL, 10), Map.entry(Mix.Op.DELETE, 10));
        assertThat(Mix.parse(Mix.DEFAULT).weights()).isEqualTo(mix.weights());
    }

    @Test
    void drawsMatchWeightsWithinOnePercent() {
        Mix mix = Mix.parse(Mix.DEFAULT);
        Map<Mix.Op, Integer> counts = new EnumMap<>(Mix.Op.class);
        Random rnd = new Random(42);
        int draws = 100_000;
        for (int i = 0; i < draws; i++) {
            counts.merge(mix.draw(rnd), 1, Integer::sum);
        }
        mix.weights().forEach((op, weight) ->
                assertThat(100.0 * counts.getOrDefault(op, 0) / draws).as("%s", op).isCloseTo(weight, org.assertj.core.data.Offset.offset(1.0)));
    }

    @Test
    void weightsNeedNotSumTo100AndZeroDisablesAnOp() {
        Mix mix = Mix.parse("insert=1,delete=1,title=0");
        Random rnd = new Random(1);
        for (int i = 0; i < 1000; i++) {
            assertThat(mix.draw(rnd)).isIn(Mix.Op.INSERT, Mix.Op.DELETE);
        }
    }

    @Test
    void rejectsBadMixes() {
        assertThatThrownBy(() -> Mix.parse("insert=5,teleport=3")).hasMessageContaining("teleport");
        assertThatThrownBy(() -> Mix.parse("insert=-1")).hasMessageContaining("insert");
        assertThatThrownBy(() -> Mix.parse("insert=0")).hasMessageContaining("at least one");
    }
}
