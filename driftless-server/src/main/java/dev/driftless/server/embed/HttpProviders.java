package dev.driftless.server.embed;

import java.net.http.HttpClient;
import java.time.Duration;
import java.util.function.Supplier;
import org.springframework.http.client.JdkClientHttpRequestFactory;
import org.springframework.web.client.HttpStatusCodeException;
import org.springframework.web.client.ResourceAccessException;
import org.springframework.web.client.RestClient;

/** Shared HTTP plumbing for the real providers: timeouts and mapping errors to retryable or not. */
final class HttpProviders {

    private HttpProviders() { }

    static RestClient client(String baseUrl) {
        HttpClient http = HttpClient.newBuilder().version(HttpClient.Version.HTTP_1_1).connectTimeout(Duration.ofSeconds(5)).build();
        JdkClientHttpRequestFactory factory = new JdkClientHttpRequestFactory(http);
        factory.setReadTimeout(Duration.ofSeconds(120));
        return RestClient.builder().baseUrl(baseUrl).requestFactory(factory).build();
    }

    static <T> T call(String provider, Supplier<T> request) {
        try {
            return request.get();
        } catch (HttpStatusCodeException e) {
            int status = e.getStatusCode().value();
            // 401/403 is a key or permission problem for every row, not this one: wait for it to be fixed
            if (status == 429 || status >= 500 || status == 401 || status == 403) {
                throw new ProviderUnavailableException(provider + " returned " + status, e);
            }
            throw new IllegalArgumentException(provider + " returned " + status + ": " + e.getResponseBodyAsString(), e);
        } catch (ResourceAccessException e) {
            throw new ProviderUnavailableException(provider + " is unreachable: " + e.getMessage(), e);
        }
    }

    static float[] floats(tools.jackson.databind.JsonNode array) {
        float[] v = new float[array.size()];
        for (int i = 0; i < v.length; i++) {
            v[i] = (float) array.get(i).asDouble();
        }
        return v;
    }
}
