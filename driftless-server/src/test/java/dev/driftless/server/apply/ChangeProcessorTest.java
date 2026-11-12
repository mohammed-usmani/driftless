package dev.driftless.server.apply;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import dev.driftless.server.change.ChangeEvent;
import dev.driftless.server.change.Op;
import dev.driftless.server.change.Version;
import dev.driftless.server.embed.EmbedResult;
import dev.driftless.server.embed.EmbeddingService;
import dev.driftless.server.embed.EmbeddingSpec;
import dev.driftless.server.embed.ProviderUnavailableException;
import dev.driftless.server.generation.Generation;
import dev.driftless.server.generation.GenerationRepository;
import dev.driftless.server.generation.GenerationStatus;
import dev.driftless.server.generation.RowStateRepository;
import java.time.Clock;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

class ChangeProcessorTest {

    private final Generation g = new Generation(1, "p", 1, new EmbeddingSpec("fake", "f", 2), "{title}", 2000, 200,
            List.of(), "h", GenerationStatus.LIVE, "chunks_p_g1");
    private final GenerationRepository generations = mock(GenerationRepository.class);
    private final RowStateRepository rows = mock(RowStateRepository.class);
    private final EmbeddingService embeddings = mock(EmbeddingService.class);
    private final ChangeApplier applier = mock(ChangeApplier.class);
    private final FailedChangeRepository failed = mock(FailedChangeRepository.class);
    private final RecentChangeRepository recent = mock(RecentChangeRepository.class);
    private final List<ChangeEvent> applied = new ArrayList<>();
    private ChangeProcessor processor;

    @BeforeEach
    void setUp() {
        when(generations.targets("p")).thenReturn(List.of(g));
        when(rows.find(anyLong(), anyString())).thenReturn(Optional.empty());
        when(embeddings.embed(any(), anyList())).thenAnswer(inv -> {
            List<String> chunks = inv.getArgument(1);
            return new EmbedResult(chunks.stream().map(c -> new float[2]).toList(), 0, chunks.size(), 1,
                    new boolean[chunks.size()]);
        });
        when(applier.apply(any(), any(), any(), any())).thenAnswer(inv -> {
            ChangeEvent e = inv.getArgument(1);
            applied.add(e);
            return e.op() == Op.TRUNCATE ? Outcome.TRUNCATE : Outcome.EMBED;
        });
        processor = new ChangeProcessor(generations, rows, new Preparer(), embeddings, applier, failed, recent, Clock.systemUTC());
    }

    private static ChangeEvent upsert(String key, long v, String title) {
        return new ChangeEvent("p", Op.UPSERT, key, Map.of("title", title), new Version(v, 0), Instant.now(), false, false);
    }

    @Test
    void coalescesToNewestPerKey() {
        processor.process("p", List.of(upsert("1", 1, "a"), upsert("1", 3, "c"), upsert("1", 2, "b"), upsert("2", 1, "x")));
        assertThat(applied).extracting(ChangeEvent::key, e -> e.version().major()).containsExactlyInAnyOrder(
                org.assertj.core.groups.Tuple.tuple("1", 3L), org.assertj.core.groups.Tuple.tuple("2", 1L));
    }

    @Test
    void truncateSplitsBatch() {
        ChangeEvent truncate = new ChangeEvent("p", Op.TRUNCATE, null, null, new Version(5, 0), Instant.now(), false, false);
        processor.process("p", List.of(upsert("1", 1, "a"), truncate, upsert("1", 9, "b")));
        assertThat(applied).extracting(ChangeEvent::op, e -> e.version().major()).containsExactly(
                org.assertj.core.groups.Tuple.tuple(Op.UPSERT, 1L), org.assertj.core.groups.Tuple.tuple(Op.TRUNCATE, 5L),
                org.assertj.core.groups.Tuple.tuple(Op.UPSERT, 9L));
    }

    @Test
    void embedsAllChunksOfABatchTogether() {
        processor.process("p", List.of(upsert("1", 1, "a"), upsert("2", 1, "b")));
        ArgumentCaptor<List<String>> chunks = ArgumentCaptor.captor();
        verify(embeddings).embed(eq(g.embedding()), chunks.capture());
        assertThat(chunks.getValue()).containsExactlyInAnyOrder("a", "b");
    }

    @Test
    void failedOutcomeRecorded() {
        ChangeEvent broken = new ChangeEvent("p", Op.UPSERT, "1", Map.of(), new Version(1, 0), Instant.now(), false, false);
        org.mockito.Mockito.doReturn(Outcome.FAILED).when(applier).apply(any(), eq(broken), any(), any());
        List<Applied> result = processor.process("p", List.of(broken));
        verify(failed).record(eq("p"), eq("1"), eq(new Version(1, 0)), eq("unknown column: title"), any());
        assertThat(result).extracting(Applied::outcome).containsExactly(Outcome.FAILED);
    }

    @Test
    void providerDownPropagates() {
        when(embeddings.embed(any(), anyList())).thenThrow(new ProviderUnavailableException("down"));
        assertThatThrownBy(() -> processor.process("p", List.of(upsert("1", 1, "a"))))
                .isInstanceOf(ProviderUnavailableException.class);
    }
}
