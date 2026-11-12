package dev.driftless.server.source;

import static org.assertj.core.api.Assertions.assertThat;

import dev.driftless.server.config.PipelineConfig;
import dev.driftless.server.support.IntegrationTest;
import dev.driftless.server.support.Pipelines;
import dev.driftless.server.support.SourceDb;
import java.util.List;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.jdbc.DataSourceBuilder;
import org.springframework.jdbc.core.JdbcTemplate;

class SourcePreflightIT extends IntegrationTest {

    private static final String T = "pf_articles";
    private final PipelineConfig pipeline = Pipelines.articles(T);

    @Autowired
    SourcePreflight preflight;

    @BeforeEach
    void freshTable() {
        SourceDb.createArticles(sourceJdbc(), T);
    }

    @Test
    void readyTableHasNoProblems() {
        assertThat(preflight.check(pipeline)).isEmpty();
    }

    @Test
    void defaultReplicaIdentityIsReported() {
        sourceJdbc().execute("ALTER TABLE " + T + " REPLICA IDENTITY DEFAULT");
        assertOneProblem(preflight.check(pipeline), "REPLICA IDENTITY FULL");
    }

    @Test
    void signalTableOutsideThePublicationIsReported() {
        sourceJdbc().execute("ALTER PUBLICATION driftless_" + T + " DROP TABLE driftless_signal");
        List<String> problems = preflight.check(pipeline);
        assertOneProblem(problems, "driftless_signal");
        assertThat(problems.getFirst()).contains("publication");
    }

    @Test
    void tableOutsideThePublicationIsReported() {
        sourceJdbc().execute("ALTER PUBLICATION driftless_" + T + " DROP TABLE " + T);
        assertOneProblem(preflight.check(pipeline), "publication");
    }

    @Test
    void missingTableIsReported() {
        PipelineConfig missing = new PipelineConfig(T, "public.pf_nope", "id", "{title}", List.of(),
                pipeline.embedding(), pipeline.chunking());
        assertOneProblem(preflight.check(missing), "does not exist");
    }

    @Test
    void userWithoutInsertOnTheSignalTableIsReported() {
        sourceJdbc().execute("DROP ROLE IF EXISTS pf_reader");
        sourceJdbc().execute("CREATE ROLE pf_reader LOGIN PASSWORD 'pf'");
        sourceJdbc().execute("GRANT SELECT ON " + T + ", driftless_signal TO pf_reader");
        JdbcTemplate restricted = new JdbcTemplate(DataSourceBuilder.create().url(sourceJdbcUrl())
                .username("pf_reader").password("pf").build());
        assertOneProblem(new SourcePreflight(restricted).check(pipeline), "INSERT");
    }

    private static void assertOneProblem(List<String> problems, String fragment) {
        assertThat(problems).hasSize(1);
        assertThat(problems.getFirst()).contains(fragment);
    }
}
