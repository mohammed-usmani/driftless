package dev.driftless.server.embed;

import dev.driftless.server.config.DriftlessProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/** Providers are beans named after the {@code provider} value pipelines configure. */
@Configuration(proxyBeanMethods = false)
class EmbeddingBeans {

    @Bean("ollama")
    EmbeddingProvider ollama(DriftlessProperties props) {
        return new OllamaProvider(props.embedding().ollamaUrl());
    }

    @Bean("openai")
    EmbeddingProvider openai(DriftlessProperties props) {
        return new OpenAiProvider(props.embedding().openaiUrl(), props.embedding().openaiKey() == null ? "" : props.embedding().openaiKey());
    }

    @Bean("fake")
    EmbeddingProvider fake(DriftlessProperties props) {
        return new FakeProvider(props.embedding().fakeLatencyMs());
    }
}
