package dev.driftless.server.embed;

import static com.github.tomakehurst.wiremock.client.WireMock.aResponse;
import static com.github.tomakehurst.wiremock.client.WireMock.equalTo;
import static com.github.tomakehurst.wiremock.client.WireMock.equalToJson;
import static com.github.tomakehurst.wiremock.client.WireMock.post;
import static com.github.tomakehurst.wiremock.client.WireMock.postRequestedFor;
import static com.github.tomakehurst.wiremock.client.WireMock.urlEqualTo;
import static com.github.tomakehurst.wiremock.core.WireMockConfiguration.wireMockConfig;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.github.tomakehurst.wiremock.WireMockServer;
import java.util.List;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/** Request and response shapes of the real providers, against WireMock. */
class ProvidersIT {

    static final WireMockServer server = new WireMockServer(wireMockConfig().dynamicPort());
    final EmbeddingSpec spec = new EmbeddingSpec("ollama", "nomic-embed-text", 2);

    @BeforeAll
    static void start() {
        server.start();
    }

    @AfterAll
    static void stop() {
        server.stop();
    }

    @BeforeEach
    void reset() {
        server.resetAll();
    }

    private String url() {
        return "http://localhost:" + server.port();
    }

    @Test
    void ollamaEmbedsABatch() {
        server.stubFor(post("/api/embed").willReturn(aResponse().withHeader("Content-Type", "application/json")
                .withBody("{\"embeddings\":[[0.1,0.2],[0.3,0.4]]}")));
        List<float[]> v = new OllamaProvider(url()).embed(spec, List.of("a", "b"));
        assertThat(v).hasSize(2);
        assertThat(v.get(1)).containsExactly(0.3f, 0.4f);
        server.verify(postRequestedFor(urlEqualTo("/api/embed"))
                .withRequestBody(equalToJson("{\"model\":\"nomic-embed-text\",\"input\":[\"a\",\"b\"]}")));
    }

    @Test
    void openAiEmbedsABatchWithItsKey() {
        server.stubFor(post("/v1/embeddings").willReturn(aResponse().withHeader("Content-Type", "application/json")
                .withBody("{\"data\":[{\"index\":1,\"embedding\":[0.3,0.4]},{\"index\":0,\"embedding\":[0.1,0.2]}]}")));
        List<float[]> v = new OpenAiProvider(url(), "sk-test").embed(new EmbeddingSpec("openai", "text-embedding-3-small", 2), List.of("a", "b"));
        assertThat(v.get(0)).containsExactly(0.1f, 0.2f);
        assertThat(v.get(1)).containsExactly(0.3f, 0.4f);
        server.verify(postRequestedFor(urlEqualTo("/v1/embeddings"))
                .withHeader("Authorization", equalTo("Bearer sk-test"))
                .withRequestBody(equalToJson("{\"model\":\"text-embedding-3-small\",\"input\":[\"a\",\"b\"]}")));
    }

    @Test
    void serverErrorsAndRateLimitsMeanUnavailable() {
        server.stubFor(post("/api/embed").willReturn(aResponse().withStatus(503)));
        assertThatThrownBy(() -> new OllamaProvider(url()).embed(spec, List.of("a")))
                .isInstanceOf(ProviderUnavailableException.class);
        server.stubFor(post("/api/embed").willReturn(aResponse().withStatus(429)));
        assertThatThrownBy(() -> new OllamaProvider(url()).embed(spec, List.of("a")))
                .isInstanceOf(ProviderUnavailableException.class);
    }

    @Test
    void connectionRefusedMeansUnavailable() {
        assertThatThrownBy(() -> new OllamaProvider("http://localhost:1").embed(spec, List.of("a")))
                .isInstanceOf(ProviderUnavailableException.class);
    }

    @Test
    void badRequestIsNotRetryable() {
        server.stubFor(post("/api/embed").willReturn(aResponse().withStatus(400).withBody("model not found")));
        assertThatThrownBy(() -> new OllamaProvider(url()).embed(spec, List.of("a")))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("400");
    }
}
