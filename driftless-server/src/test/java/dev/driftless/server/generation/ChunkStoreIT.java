package dev.driftless.server.generation;

import static org.assertj.core.api.Assertions.assertThat;

import dev.driftless.server.change.Version;
import dev.driftless.server.config.PipelineConfig;
import dev.driftless.server.embed.EmbeddingSpec;
import dev.driftless.server.support.IntegrationTest;
import dev.driftless.server.support.Pipelines;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;

class ChunkStoreIT extends IntegrationTest {

    @Autowired GenerationRepository generations;
    @Autowired ChunkStore chunks;

    private Generation g;

    @BeforeEach
    void generation() {
        PipelineConfig p = Pipelines.articles("cs_" + System.nanoTime() % 1_000_000, new EmbeddingSpec("fake", "fake-768", 768));
        generations.upsertPipeline(p, PipelineStatus.ACTIVE, null);
        g = generations.create(p, GenerationStatus.LIVE);
        chunks.createTable(g);
    }

    private static List<Chunk> chunks(int n) {
        List<Chunk> out = new ArrayList<>();
        for (int i = 0; i < n; i++) {
            float[] v = new float[768];
            v[i] = 1;
            out.add(new Chunk(i, "text " + i, "hash" + i, v));
        }
        return out;
    }

    private List<Integer> chunkNos(String key) {
        return jdbc.queryForList("SELECT chunk_no FROM " + g.chunkTable() + " WHERE row_key = ? ORDER BY chunk_no", Integer.class, key);
    }

    @Test
    void replacingWithFewerChunksRemovesTheRest() {
        chunks.replace(g, "1", chunks(5), Map.of("category", "a"), new Version(1, 1));
        chunks.replace(g, "1", chunks(3), Map.of("category", "a"), new Version(2, 2));
        assertThat(chunkNos("1")).containsExactly(0, 1, 2);
        assertThat(jdbc.queryForObject("SELECT version_major FROM " + g.chunkTable() + " WHERE row_key = '1' AND chunk_no = 2", Long.class))
                .isEqualTo(2);
    }

    @Test
    void deleteRowRemovesEveryChunk() {
        chunks.replace(g, "1", chunks(3), Map.of(), new Version(1, 1));
        chunks.replace(g, "2", chunks(1), Map.of(), new Version(1, 1));
        chunks.deleteRow(g, "1");
        assertThat(chunkNos("1")).isEmpty();
        assertThat(chunkNos("2")).containsExactly(0);
    }

    @Test
    void updateMetadataChangesOnlyMetadata() {
        chunks.replace(g, "1", chunks(2), Map.of("acl", List.of("u1")), new Version(1, 1));
        String before = jdbc.queryForObject("SELECT embedding::text FROM " + g.chunkTable() + " WHERE row_key = '1' AND chunk_no = 0", String.class);
        chunks.updateMetadata(g, "1", Map.of("acl", List.of("u2")));
        assertThat(jdbc.queryForList("SELECT metadata->'acl'->>0 FROM " + g.chunkTable() + " WHERE row_key = '1'", String.class))
                .containsOnly("u2");
        assertThat(jdbc.queryForObject("SELECT embedding::text FROM " + g.chunkTable() + " WHERE row_key = '1' AND chunk_no = 0", String.class))
                .isEqualTo(before);
    }

    @Test
    void truncateEmptiesTheTable() {
        chunks.replace(g, "1", chunks(2), Map.of(), new Version(1, 1));
        chunks.truncate(g);
        assertThat(jdbc.queryForObject("SELECT count(*) FROM " + g.chunkTable(), Long.class)).isZero();
    }

    @Test
    void tableHasTypedVectorAndIndexes() {
        assertThat(jdbc.queryForObject("""
                SELECT format_type(atttypid, atttypmod) FROM pg_attribute
                WHERE attrelid = ?::regclass AND attname = 'embedding'""", String.class, g.chunkTable()))
                .isEqualTo("vector(768)");
        List<String> indexes = jdbc.queryForList("SELECT indexdef FROM pg_indexes WHERE tablename = ?", String.class, g.chunkTable());
        assertThat(indexes).anyMatch(d -> d.contains("hnsw") && d.contains("vector_cosine_ops"));
        assertThat(indexes).anyMatch(d -> d.contains("gin") && d.contains("metadata"));
    }

    @Test
    void dropTableRemovesIt() {
        chunks.dropTable(g);
        assertThat(jdbc.queryForObject("SELECT to_regclass(?) IS NULL", Boolean.class, g.chunkTable())).isTrue();
    }
}
