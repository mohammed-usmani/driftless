package dev.driftless.server.embed;

import java.util.List;
import java.util.Map;
import org.springframework.web.client.RestClient;
import tools.jackson.databind.JsonNode;

/** OpenAI's {@code POST /v1/embeddings}. Results are placed by their {@code index}, not their order. */
public class OpenAiProvider implements EmbeddingProvider {

    private final RestClient http;

    public OpenAiProvider(String baseUrl, String apiKey) {
        this.http = HttpProviders.client(baseUrl).mutate().defaultHeader("Authorization", "Bearer " + apiKey).build();
    }

    @Override
    public List<float[]> embed(EmbeddingSpec spec, List<String> texts) {
        JsonNode body = HttpProviders.call("openai", () -> http.post().uri("/v1/embeddings")
                .body(Map.of("model", spec.model(), "input", texts)).retrieve().body(JsonNode.class));
        float[][] out = new float[texts.size()][];
        body.path("data").forEach(d -> out[d.path("index").asInt()] = HttpProviders.floats(d.path("embedding")));
        return List.of(out);
    }
}
