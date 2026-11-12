package dev.driftless.server.support;

import dev.driftless.server.embed.EmbeddingProvider;
import dev.driftless.server.embed.EmbeddingSpec;
import dev.driftless.server.embed.FakeProvider;
import dev.driftless.server.embed.ProviderUnavailableException;
import java.time.Instant;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;

/** The fake provider with a call counter and an outage switch. Replaces the "fake" bean where imported. */
public class ControllableFake implements EmbeddingProvider {

    public final AtomicInteger texts = new AtomicInteger();
    public volatile Instant downUntil = Instant.MIN;
    private final FakeProvider fake = new FakeProvider(0);

    @Override
    public List<float[]> embed(EmbeddingSpec spec, List<String> in) {
        if (Instant.now().isBefore(downUntil)) {
            throw new ProviderUnavailableException("fake outage");
        }
        texts.addAndGet(in.size());
        return fake.embed(spec, in);
    }

    /** Swaps the "fake" provider bean for a controllable one; inject it as {@code @Qualifier("fake") EmbeddingProvider}. */
    @TestConfiguration(proxyBeanMethods = false)
    public static class Config {
        @Bean
        static org.springframework.beans.factory.config.BeanPostProcessor controllableFake() {
            return new org.springframework.beans.factory.config.BeanPostProcessor() {
                @Override
                public Object postProcessAfterInitialization(Object bean, String name) {
                    return name.equals("fake") ? new ControllableFake() : bean;
                }
            };
        }
    }
}
