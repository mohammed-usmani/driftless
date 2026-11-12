package dev.driftless.server.source;

import com.zaxxer.hikari.HikariDataSource;
import dev.driftless.server.config.DriftlessProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.jdbc.core.JdbcTemplate;

/**
 * The source database, read by preflight, the reconciler and signal inserts. It is deliberately not a
 * {@code DataSource} bean, so Spring Boot keeps auto-configuring the index database as the primary one.
 */
@Configuration(proxyBeanMethods = false)
public class SourceDatabase {

    /** Holder so the source template can't be injected by accident where the index one is meant. */
    public record Source(JdbcTemplate jdbc, String host, int port, String database) { }

    @Bean(destroyMethod = "")
    Source source(DriftlessProperties props) {
        DriftlessProperties.Source s = props.source();
        HikariDataSource ds = new HikariDataSource();
        ds.setJdbcUrl(s.url());
        ds.setUsername(s.user());
        ds.setPassword(s.password());
        ds.setMaximumPoolSize(4);
        ds.setPoolName("source");
        // jdbc:postgresql://host:port/db?params
        String rest = s.url().substring("jdbc:postgresql://".length());
        String hostPort = rest.substring(0, rest.indexOf('/'));
        String db = rest.substring(rest.indexOf('/') + 1).split("\\?")[0];
        String host = hostPort.contains(":") ? hostPort.substring(0, hostPort.indexOf(':')) : hostPort;
        int port = hostPort.contains(":") ? Integer.parseInt(hostPort.substring(hostPort.indexOf(':') + 1)) : 5432;
        return new Source(new JdbcTemplate(ds),
                s.connectHost() != null ? s.connectHost() : host,
                s.connectPort() != null ? s.connectPort() : port,
                db);
    }
}
