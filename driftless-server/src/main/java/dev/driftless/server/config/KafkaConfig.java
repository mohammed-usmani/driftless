package dev.driftless.server.config;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.listener.CommonErrorHandler;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.util.backoff.FixedBackOff;

@Configuration(proxyBeanMethods = false)
class KafkaConfig {

    /**
     * A failed batch is retried forever, never skipped: skipping a change would leave the index wrong with
     * nothing to show for it. Per-row problems never reach here; they become failed_change rows instead.
     */
    @Bean
    CommonErrorHandler errorHandler() {
        return new DefaultErrorHandler(new FixedBackOff(1_000, FixedBackOff.UNLIMITED_ATTEMPTS));
    }
}
