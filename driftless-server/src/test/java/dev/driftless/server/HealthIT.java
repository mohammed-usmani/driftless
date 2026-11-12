package dev.driftless.server;

import static org.assertj.core.api.Assertions.assertThat;

import dev.driftless.server.support.IntegrationTest;
import org.junit.jupiter.api.Test;
import org.springframework.http.ResponseEntity;

class HealthIT extends IntegrationTest {

    @Test
    void readinessIsUp() {
        ResponseEntity<String> res = management().get().uri("/actuator/health/readiness")
                .retrieve().toEntity(String.class);
        assertThat(res.getStatusCode().value()).isEqualTo(200);
        assertThat(res.getBody()).contains("\"status\":\"UP\"");
    }
}
