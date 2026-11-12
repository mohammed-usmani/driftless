package dev.driftless.server.config;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.charset.StandardCharsets;
import org.junit.jupiter.api.Test;
import org.springframework.boot.autoconfigure.context.ConfigurationPropertiesAutoConfiguration;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.env.YamlPropertySourceLoader;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.boot.validation.autoconfigure.ValidationAutoConfiguration;
import org.springframework.context.annotation.Configuration;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.core.env.PropertySource;
import org.springframework.core.io.ByteArrayResource;

class DriftlessPropertiesTest {

    // the spec's configuration example, plus the dimensions every pipeline must declare
    private static final String SPEC_EXAMPLE = """
            driftless:
              pipelines:
                - name: articles
                  table: public.articles
                  key: id
                  text: "{title}\\n\\n{body}"
                  metadata: [category, tenant_id, acl]
                  embedding: { provider: ollama, model: nomic-embed-text, dimensions: 768 }
                  chunking: { max-chars: 2000, overlap-chars: 200 }
            """;

    @Configuration
    @EnableConfigurationProperties(DriftlessProperties.class)
    static class Config { }

    private ApplicationContextRunner runner(String yaml) {
        return new ApplicationContextRunner()
                .withConfiguration(AutoConfigurations.of(
                        ConfigurationPropertiesAutoConfiguration.class, ValidationAutoConfiguration.class))
                .withUserConfiguration(Config.class)
                .withInitializer(ctx -> ctx.getEnvironment().getPropertySources().addFirst(yaml(yaml)));
    }

    private static PropertySource<?> yaml(String yaml) {
        try {
            return new YamlPropertySourceLoader()
                    .load("test", new ByteArrayResource(yaml.getBytes(StandardCharsets.UTF_8))).getFirst();
        } catch (java.io.IOException e) {
            throw new IllegalStateException(e);
        }
    }

    @Test
    void bindsTheSpecExample() {
        runner(SPEC_EXAMPLE).run(ctx -> {
            assertThat(ctx).hasNotFailed();
            PipelineConfig p = ctx.getBean(DriftlessProperties.class).pipelines().getFirst();
            assertThat(p.name()).isEqualTo("articles");
            assertThat(p.text()).isEqualTo("{title}\n\n{body}");
            assertThat(p.metadata()).containsExactly("category", "tenant_id", "acl");
            assertThat(p.chunking().maxChars()).isEqualTo(2000);
            assertThat(p.chunking().overlapChars()).isEqualTo(200);
            assertThat(p.embedding().dimensions()).isEqualTo(768);
            assertThat(p.schema()).isEqualTo("public");
            assertThat(p.tableName()).isEqualTo("articles");
        });
    }

    @Test
    void rejectsDimensionsAboveTheHnswLimit() {
        runner(SPEC_EXAMPLE.replace("dimensions: 768", "dimensions: 3072")).run(ctx -> {
            assertThat(ctx).hasFailed();
            assertThat(rootMessages(ctx.getStartupFailure())).contains("2000");
        });
    }

    @Test
    void rejectsPipelineNamesThatAreNotSafeIdentifiers() {
        runner(SPEC_EXAMPLE.replace("name: articles", "name: Articles-1; drop")).run(ctx -> assertThat(ctx).hasFailed());
    }

    private static String rootMessages(Throwable t) {
        StringBuilder sb = new StringBuilder();
        for (Throwable c = t; c != null; c = c.getCause()) {
            sb.append(c.getMessage()).append('\n');
        }
        return sb.toString();
    }
}
