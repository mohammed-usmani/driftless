package dev.driftless.server.embed;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.springframework.web.client.RestClient;
import tools.jackson.databind.JsonNode;

/** Ollama's {@code POST /api/embed}. */
public class OllamaProvider implements EmbeddingProvider {

    private final RestClient http;

    public OllamaProvider(String baseUrl) {
        this.http = HttpProviders.client(baseUrl);
    }

    @Override
    public List<float[]> embed(EmbeddingSpec spec, List<String> texts) {
        JsonNode body = HttpProviders.call("ollama", () -> http.post().uri("/api/embed")
                .body(Map.of("model", spec.model(), "input", texts)).retrieve().body(JsonNode.class));
        List<float[]> out = new ArrayList<>(texts.size());
        body.path("embeddings").forEach(e -> out.add(HttpProviders.floats(e)));
        return out;
    }
}
