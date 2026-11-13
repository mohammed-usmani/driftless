package dev.driftless.server.search;

import dev.driftless.server.api.ApiException;
import dev.driftless.server.api.NotFoundException;
import dev.driftless.server.config.DriftlessProperties;
import dev.driftless.server.embed.EmbeddingService;
import dev.driftless.server.embed.Vectors;
import dev.driftless.server.generation.ChunkStore;
import dev.driftless.server.generation.Generation;
import dev.driftless.server.generation.GenerationRepository;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import org.springframework.http.HttpStatus;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;
import tools.jackson.core.type.TypeReference;
import tools.jackson.databind.json.JsonMapper;

@Service
public class SearchService {

    private static final JsonMapper JSON = JsonMapper.builder().build();
    private static final String ACL = "acl";

    public record Hit(String key, int chunkNo, double score, String text, Map<String, Object> metadata) { }

    public record Result(int generation, List<Hit> results) { }

    private final DriftlessProperties props;
    private final GenerationRepository generations;
    private final EmbeddingService embeddings;
    private final JdbcTemplate jdbc;
    private final TransactionTemplate readOnly;

    public SearchService(DriftlessProperties props, GenerationRepository generations, EmbeddingService embeddings, JdbcTemplate jdbc,
                         PlatformTransactionManager transactions) {
        this.props = props;
        this.generations = generations;
        this.embeddings = embeddings;
        this.jdbc = jdbc;
        this.readOnly = new TransactionTemplate(transactions);
        this.readOnly.setReadOnly(true);
    }

    /**
     * Nearest chunks of the live generation. pgvector filters after the HNSW scan, so a selective filter
     * would return fewer than k rows; iterative scan keeps scanning until k rows pass the filter.
     * The query is embedded before any transaction opens, so a slow provider never holds a pooled connection.
     */
    public Result search(String pipeline, String query, int k, Map<String, Object> filter, String principal) {
        if (props.pipeline(pipeline) == null) {
            throw new NotFoundException("no pipeline " + pipeline);
        }
        Generation g = generations.live(pipeline).orElseThrow(() -> new NotFoundException("pipeline " + pipeline + " has no live generation"));

        List<String> where = new ArrayList<>();
        List<Object> args = new ArrayList<>();
        args.add(null); // the query vector, filled in once the request is known to be valid
        (filter == null ? Map.<String, Object>of() : filter).forEach((col, value) -> {
            if (!g.metadataColumns().contains(col)) {
                throw new ApiException(HttpStatus.BAD_REQUEST, "cannot filter on " + col + "; filterable columns: " + g.metadataColumns());
            }
            List<String> any = new ArrayList<>();
            for (Object v : value instanceof Collection<?> c ? c : List.of(value)) {
                // a scalar column equals v, or an array column contains v
                any.add("metadata @> ?::jsonb OR metadata @> ?::jsonb");
                args.add(JSON.writeValueAsString(Map.of(col, v)));
                args.add(JSON.writeValueAsString(Map.of(col, List.of(v))));
            }
            where.add(any.isEmpty() ? "false" : "(" + String.join(" OR ", any) + ")");
        });
        if (principal != null) {
            if (!g.metadataColumns().contains(ACL)) {
                throw new ApiException(HttpStatus.BAD_REQUEST, "pipeline " + pipeline + " has no acl metadata column to check a principal against");
            }
            where.add("metadata @> ?::jsonb");
            args.add(JSON.writeValueAsString(Map.of(ACL, List.of(principal))));
        }
        args.add(k);
        args.set(0, Vectors.toText(embeddings.embedQuery(g.embedding(), query)));
        return readOnly.execute(tx -> query(g, where, args));
    }

    private Result query(Generation g, List<String> where, List<Object> args) {
        jdbc.execute("SET LOCAL hnsw.iterative_scan = relaxed_order");
        jdbc.execute("SET LOCAL hnsw.ef_search = 100");
        // relaxed_order can return slightly out of order; the outer ORDER BY fixes that
        String sql = """
                WITH r AS MATERIALIZED (
                    SELECT row_key, chunk_no, text, metadata::text AS metadata, embedding <=> ?::vector AS distance
                    FROM %s %s ORDER BY distance LIMIT ?)
                SELECT * FROM r ORDER BY distance""".formatted(ChunkStore.table(g), where.isEmpty() ? "" : "WHERE " + String.join(" AND ", where));
        List<Hit> hits = jdbc.query(sql, (rs, i) -> new Hit(rs.getString("row_key"), rs.getInt("chunk_no"), 1 - rs.getDouble("distance"),
                rs.getString("text"), JSON.readValue(rs.getString("metadata"), new TypeReference<Map<String, Object>>() { })), args.toArray());
        return new Result(g.number(), hits);
    }
}
