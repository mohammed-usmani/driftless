package dev.driftless.server;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.ConfigurationPropertiesScan;

@SpringBootApplication
@ConfigurationPropertiesScan
public class DriftlessApplication {

    public static void main(String[] args) {
        SpringApplication.run(DriftlessApplication.class, args);
    }
}
