package dev.driftless.server.support;

import java.sql.DriverManager;
import java.time.Duration;
import javax.sql.DataSource;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.jdbc.DataSourceBuilder;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.boot.testcontainers.service.connection.ServiceConnection;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.web.client.RestClient;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.postgresql.PostgreSQLContainer;
import org.testcontainers.utility.DockerImageName;

/**
 * Base for integration tests. One Postgres (two databases: {@code driftless} is the index, {@code source}
 * is the synced side), Kafka, and Debezium Connect are started once per JVM and shared by every context.
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@ActiveProfiles("test")
public abstract class IntegrationTest {

    protected static final Network NETWORK = Network.newNetwork();

    @ServiceConnection
    protected static final PostgreSQLContainer POSTGRES = new PostgreSQLContainer(
            DockerImageName.parse("pgvector/pgvector:pg17").asCompatibleSubstituteFor("postgres"))
            .withDatabaseName("driftless")
            .withNetwork(NETWORK)
            .withNetworkAliases("pg")
            .withCommand("postgres", "-c", "wal_level=logical", "-c", "max_replication_slots=20",
                    "-c", "max_wal_senders=20", "-c", "fsync=off");

    @ServiceConnection
    protected static final KafkaContainer KAFKA = new KafkaContainer("apache/kafka:4.1.0")
            .withNetwork(NETWORK)
            .withListener("kafka:19092");

    protected static final GenericContainer<?> CONNECT = new GenericContainer<>(
            "quay.io/debezium/connect:" + System.getProperty("debezium.version", "3.7.1.Final"))
            .withNetwork(NETWORK)
            .withExposedPorts(8083)
            .withEnv("BOOTSTRAP_SERVERS", "kafka:19092")
            .withEnv("GROUP_ID", "connect")
            .withEnv("CONFIG_STORAGE_TOPIC", "connect_configs")
            .withEnv("OFFSET_STORAGE_TOPIC", "connect_offsets")
            .withEnv("STATUS_STORAGE_TOPIC", "connect_statuses")
            .withEnv("CONFIG_STORAGE_REPLICATION_FACTOR", "1")
            .withEnv("OFFSET_STORAGE_REPLICATION_FACTOR", "1")
            .withEnv("STATUS_STORAGE_REPLICATION_FACTOR", "1")
            .withEnv("OFFSET_FLUSH_INTERVAL_MS", "1000")
            .waitingFor(Wait.forHttp("/connectors").forPort(8083).withStartupTimeout(Duration.ofMinutes(3)));

    static {
        POSTGRES.start();
        KAFKA.start();
        CONNECT.dependsOn(KAFKA).start();
        try (var c = DriverManager.getConnection(POSTGRES.getJdbcUrl(), POSTGRES.getUsername(), POSTGRES.getPassword());
                var s = c.createStatement()) {
            s.execute("CREATE DATABASE source");
        } catch (java.sql.SQLException e) {
            throw new IllegalStateException(e);
        }
    }

    protected static final String ADMIN_TOKEN = "test-admin-token";

    @DynamicPropertySource
    static void driftlessProperties(DynamicPropertyRegistry r) {
        r.add("driftless.source.url", IntegrationTest::sourceJdbcUrl);
        r.add("driftless.source.user", POSTGRES::getUsername);
        r.add("driftless.source.password", POSTGRES::getPassword);
        r.add("driftless.source.connect-host", () -> "pg");
        r.add("driftless.source.connect-port", () -> 5432);
        r.add("driftless.kafka-connect-url", IntegrationTest::connectUrl);
        r.add("management.server.port", () -> 0);
        // every cached test context gets its own consumer group, or contexts would split partitions between them
        String group = "driftless-" + java.util.UUID.randomUUID();
        r.add("driftless.consumer-group", () -> group);
    }

    protected static String sourceJdbcUrl() {
        return POSTGRES.getJdbcUrl().replace("/driftless", "/source");
    }

    protected static String connectUrl() {
        return "http://" + CONNECT.getHost() + ":" + CONNECT.getMappedPort(8083);
    }

    private static JdbcTemplate sourceJdbc;

    /** A JDBC template on the source database (the side Debezium reads). */
    protected static synchronized JdbcTemplate sourceJdbc() {
        if (sourceJdbc == null) {
            DataSource ds = DataSourceBuilder.create().url(sourceJdbcUrl())
                    .username(POSTGRES.getUsername()).password(POSTGRES.getPassword()).build();
            sourceJdbc = new JdbcTemplate(ds);
        }
        return sourceJdbc;
    }

    /** Changes committed before Debezium's slot is active are not streamed, so tests wait for it. */
    protected static void awaitSlotActive(String pipeline) {
        org.awaitility.Awaitility.await().atMost(Duration.ofMinutes(1)).until(() -> Boolean.TRUE.equals(
                sourceJdbc().queryForObject("SELECT exists(SELECT 1 FROM pg_replication_slots WHERE slot_name = ? AND active)",
                        Boolean.class, "driftless_" + pipeline)));
    }

    @Autowired
    protected JdbcTemplate jdbc;

    @LocalServerPort
    protected int port;

    @Value("${local.management.port}")
    protected int managementPort;

    /** HTTP client against the running app that never throws on 4xx/5xx. */
    protected RestClient client() {
        return RestClient.builder()
                .baseUrl("http://localhost:" + port)
                .defaultStatusHandler(status -> true, (req, res) -> { })
                .build();
    }

    protected RestClient management() {
        return RestClient.builder()
                .baseUrl("http://localhost:" + managementPort)
                .defaultStatusHandler(status -> true, (req, res) -> { })
                .build();
    }

    protected RestClient admin() {
        return client().mutate().defaultHeader("Authorization", "Bearer " + ADMIN_TOKEN).build();
    }
}
