package io.github.rieske.cdc;

import org.flywaydb.core.Flyway;
import org.junit.jupiter.api.extension.AfterEachCallback;
import org.junit.jupiter.api.extension.BeforeEachCallback;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.postgresql.ds.PGSimpleDataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.images.builder.ImageFromDockerfile;

import javax.sql.DataSource;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.SQLException;
import java.time.Duration;
import java.util.Map;
import java.util.UUID;

public class DatabaseExtension implements BeforeEachCallback, AfterEachCallback {
    private static final Logger LOGGER = LoggerFactory.getLogger(DatabaseExtension.class);

    private static final String DB_USERNAME = "test";
    private static final String DB_PASSWORD = "test";
    private static final int DB_PORT = 5432;
    private static final String DEFAULT_DATABASE = "postgres";

    private static final GenericContainer<?> DB_CONTAINER = new GenericContainer<>(
            new ImageFromDockerfile("postgres-cdc-test")
                    .withDockerfile(Path.of("./src/test/resources/postgres/Dockerfile"))
    ).withExposedPorts(DB_PORT)
            .withLogConsumer(new Slf4jLogConsumer(LOGGER).withPrefix("database"))
            .withEnv(Map.of(
                    "POSTGRES_USER", DB_USERNAME,
                    "POSTGRES_PASSWORD", DB_PASSWORD
            ))
            .waitingFor(Wait.forListeningPort().withStartupTimeout(Duration.ofSeconds(10)))
            .withReuse(true)
            ;

    private static final String JDBC_URI;

    static {
        DB_CONTAINER.start();

        JDBC_URI = "jdbc:postgresql://" + DB_CONTAINER.getHost() + ":" + DB_CONTAINER.getMappedPort(DB_PORT);

        Flyway.configure()
                .dataSource(dataSourceForDatabase(DEFAULT_DATABASE))
                .load()
                .migrate();
    }

    private final String databaseName = "unit_test_" + UUID.randomUUID().toString().replace('-', '_');

    public DataSource getDataSource() {
        return dataSourceForDatabase(databaseName);
    }

    @Override
    public void beforeEach(ExtensionContext context) {
        executeInDefaultDatabase("CREATE DATABASE " + databaseName + " TEMPLATE " + DEFAULT_DATABASE);
    }

    @Override
    public void afterEach(ExtensionContext context) {
        executeInDefaultDatabase("DROP DATABASE " + databaseName);
    }

    private static DataSource dataSourceForDatabase(String databaseName) {
        var dataSource = new PGSimpleDataSource();
        dataSource.setUrl(JDBC_URI + "/" + databaseName);
        dataSource.setUser(DB_USERNAME);
        dataSource.setPassword(DB_PASSWORD);
        return dataSource;
    }

    public String jdbcUrl() {
        return JDBC_URI + "/" + databaseName;
    }

    public String databaseUsername() {
        return DB_USERNAME;
    }

    public String databasePassword() {
        return DB_PASSWORD;
    }

    private void executeInDefaultDatabase(String sql) {
        var dataSource = dataSourceForDatabase(DEFAULT_DATABASE);
        try (Connection conn = dataSource.getConnection()) {
            conn.createStatement().execute(sql);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }
}
