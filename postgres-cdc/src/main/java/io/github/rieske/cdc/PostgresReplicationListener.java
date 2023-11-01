package io.github.rieske.cdc;

import org.postgresql.PGConnection;
import org.postgresql.PGProperty;
import org.postgresql.jdbc.PgConnection;
import org.postgresql.replication.PGReplicationStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.Supplier;

class PostgresReplicationListener implements ChangeDataCapture {
    private static final Logger LOGGER = LoggerFactory.getLogger(PostgresReplicationListener.class);

    // https://github.com/eulerto/wal2json
    private static final String WAL_OUTPUT_PLUGIN = "wal2json";

    private static final String SQLSTATE_DUPLICATE_OBJECT = "42710";

    private final String jdbcUrl;
    private final String replicationSlotName;
    private final Properties databaseConnectionProperties;

    private final ExecutorService replicationStreamExecutor;

    private final ReplicationStreamConsumer replicationStreamConsumer;


    PostgresReplicationListener(
            String jdbcUrl,
            String databaseUser,
            String databasePassword,
            String replicationSlotName,
            Set<String> tablesToListenTo,
            Consumer<ByteBuffer> consumer
    ) {
        this.jdbcUrl = jdbcUrl;
        this.replicationSlotName = replicationSlotName;

        this.databaseConnectionProperties = new Properties();
        PGProperty.USER.set(databaseConnectionProperties, databaseUser);
        PGProperty.PASSWORD.set(databaseConnectionProperties, databasePassword);
        PGProperty.ASSUME_MIN_SERVER_VERSION.set(databaseConnectionProperties, "9.4");
        PGProperty.REPLICATION.set(databaseConnectionProperties, "database");
        PGProperty.PREFER_QUERY_MODE.set(databaseConnectionProperties, "simple");

        this.replicationStreamExecutor = Executors.newSingleThreadExecutor(runnable -> {
            Thread thread = Executors.defaultThreadFactory().newThread(runnable);
            thread.setName("replication-stream-listener");
            return thread;
        });

        this.replicationStreamConsumer = new ReplicationStreamConsumer(this::createConnection, replicationSlotName, tablesToListenTo, consumer);
        Runtime.getRuntime().addShutdownHook(new Thread(replicationStreamConsumer::stop));

        replicationStreamExecutor.submit(replicationStreamConsumer);
    }

    @Override
    public void createReplicationSlot() {
        try (PgConnection connection = createConnection()) {
            LOGGER.info("Creating replications slot {}", replicationSlotName);
            connection.getReplicationAPI()
                    .createReplicationSlot()
                    .logical()
                    .withSlotName(replicationSlotName)
                    .withOutputPlugin(WAL_OUTPUT_PLUGIN)
                    .make();
            LOGGER.info("Created replications slot {}", replicationSlotName);
        } catch (SQLException e) {
            if (SQLSTATE_DUPLICATE_OBJECT.equals(e.getSQLState())) {
                LOGGER.info("Replication slot {} already exists", replicationSlotName);
            } else {
                throw new RuntimeException("Could not create replication slot", e);
            }
        }
    }

    @Override
    public void dropReplicationSlot() {
        try (PgConnection connection = createConnection()) {
            LOGGER.info("Dropping replications slot {}", replicationSlotName);
            connection.getReplicationAPI().dropReplicationSlot(replicationSlotName);
            LOGGER.info("Dropped replications slot {}", replicationSlotName);
        } catch (SQLException e) {
            throw new RuntimeException("Could not drop replication slot", e);
        }
    }

    @Override
    public void start() {
        LOGGER.info("Starting replication stream listener on slot {}", replicationSlotName);
        replicationStreamConsumer.start();
    }

    @Override
    public void stop() {
        LOGGER.info("Stopping replication stream listener on slot {}", replicationSlotName);
        replicationStreamConsumer.stop();
        this.replicationStreamExecutor.shutdown();
        try {
            if (!replicationStreamExecutor.awaitTermination(10, TimeUnit.SECONDS)) {
                replicationStreamExecutor.shutdownNow();
                LOGGER.warn("Replication stream executor was shut down forcefully");
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    private PgConnection createConnection() {
        try {
            return DriverManager.getConnection(jdbcUrl, databaseConnectionProperties).unwrap(PgConnection.class);
        } catch (SQLException e) {
            throw new RuntimeException("Could not create connection", e);
        }
    }
}

class ReplicationStreamConsumer implements Runnable {
    private static final Logger LOGGER = LoggerFactory.getLogger(ReplicationStreamConsumer.class);

    private final Supplier<PgConnection> connectionSupplier;
    private final String replicationSlotName;
    private final Set<String> tablesToListenTo;
    private final Consumer<ByteBuffer> consumer;

    private volatile boolean running = false;

    ReplicationStreamConsumer(
            Supplier<PgConnection> connectionSupplier,
            String replicationSlotName,
            Set<String> tablesToListenTo,
            Consumer<ByteBuffer> consumer
    ) {
        this.connectionSupplier = connectionSupplier;
        this.replicationSlotName = replicationSlotName;
        this.tablesToListenTo = tablesToListenTo;
        this.consumer = consumer;
    }

    void start() {
        this.running = true;
        synchronized (this) {
            notify();
        }
    }

    void stop() {
        this.running = false;
    }

    @Override
    public void run() {
        synchronized (this) {
            if (!running) {
                try {
                    wait();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    LOGGER.info("Replication slot {} consumer was interrupted while not yet started", replicationSlotName);
                    return;
                }
            }
        }
        try (PgConnection connection = connectionSupplier.get()) {
            try (PGReplicationStream stream = getStream(connection)) {
                LOGGER.info("Connected to replication slot {}", replicationSlotName);
                consumeStream(stream);
            }
        } catch (Exception e) {
            LOGGER.warn("Exception thrown in replication slot {} listener loop", replicationSlotName, e);
        }
        LOGGER.info("Replication slot {} consumer was stopped", replicationSlotName);
    }

    private void consumeStream(PGReplicationStream stream) throws SQLException {
        while (running) {
            ByteBuffer msg = stream.readPending();

            if (msg == null) {
                try {
                    TimeUnit.MILLISECONDS.sleep(10L);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new RuntimeException("Thread was interrupted", e);
                }
                continue;
            }

            try {
                consumer.accept(msg);

                stream.setAppliedLSN(stream.getLastReceiveLSN());
                stream.setFlushedLSN(stream.getLastReceiveLSN());
                stream.forceUpdateStatus();
            } catch (Exception e) {
                LOGGER.warn("Could not consume database change event", e);
            }
        }
        LOGGER.info("Replication slot {} consumer was stopped", replicationSlotName);
    }

    private PGReplicationStream getStream(PGConnection connection) throws SQLException {
        return connection.getReplicationAPI().replicationStream().logical().withSlotName(replicationSlotName)
                .withSlotOption("format-version", 2)
                .withSlotOption("include-transaction", false)
                .withSlotOption("include-timestamp", true)
                .withSlotOption("add-tables", String.join(",", tablesToListenTo))
                .withStatusInterval(10, TimeUnit.SECONDS)
                .start();
    }
}
