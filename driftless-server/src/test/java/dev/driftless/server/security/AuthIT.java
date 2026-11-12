package dev.driftless.server.security;

import static org.assertj.core.api.Assertions.assertThat;

import dev.driftless.server.support.IntegrationTest;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.springframework.core.ParameterizedTypeReference;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;

class AuthIT extends IntegrationTest {

    private ResponseEntity<Map<String, Object>> search(String authorization) {
        var req = client().post().uri("/v1/pipelines/nope/search").contentType(MediaType.APPLICATION_JSON).body(Map.of("query", "q"));
        if (authorization != null) {
            req = req.header("Authorization", authorization);
        }
        return req.retrieve().toEntity(new ParameterizedTypeReference<>() { });
    }

    @Test
    void missingTokenIs401Problem() {
        var r = search(null);
        assertThat(r.getStatusCode().value()).isEqualTo(401);
        assertThat(r.getHeaders().getContentType().toString()).startsWith("application/problem+json");
        assertThat(r.getBody()).containsEntry("status", 401).containsKey("title");
    }

    @Test
    void wrongTokenIs401() {
        assertThat(search("Bearer not-the-token").getStatusCode().value()).isEqualTo(401);
        assertThat(search(ADMIN_TOKEN).getStatusCode().value()).isEqualTo(401);
    }

    @Test
    void correctTokenGetsThrough() {
        // past auth: the pipeline doesn't exist, so the request reaches the controller and gets 404
        assertThat(search("Bearer " + ADMIN_TOKEN).getStatusCode().value()).isEqualTo(404);
    }

    @Test
    void healthIsOpenOnManagementPort() {
        assertThat(management().get().uri("/actuator/health").retrieve().toBodilessEntity().getStatusCode().value()).isEqualTo(200);
    }

    @Test
    void openApiDocsAreServed() {
        var r = client().get().uri("/v3/api-docs").retrieve().toEntity(String.class);
        assertThat(r.getStatusCode().value()).isEqualTo(200);
        assertThat(r.getBody()).contains("/v1/pipelines/{name}/search").contains("bearer");
    }
}
