package io.github.rieske.cdc;

import java.util.Set;
import java.util.function.Consumer;

/**
 * The base interface to create an instance of ChangeDataCapture service.
 * Exposes methods to create and drop replication slots and start/stop the data change stream consumption
 */
public interface ChangeDataCapture {

    /**
     * Create an instance of ChangeDataCapture service.
     *
     * @param jdbcUrl JDBC URL of the database to stream changes from
     * @param databaseUser the database user
     * @param databasePassword the database password
     * @param replicationSlotName name of the replication slot to use. The replication slot must be created before starting to stream the changes.
     * @param tablesToListenTo a Set of tables to stream changes from. Format: "schema.table"
     * @param consumer the consumer where changes will be streamed to.
     *
     * @return a ChangeDataCapture instance. Call the start() method to start streaming changes to the consumer.
     */
    static ChangeDataCapture create(
            String jdbcUrl,
            String databaseUser,
            String databasePassword,
            String replicationSlotName,
            Set<String> tablesToListenTo,
            Consumer<DatabaseChange> consumer
    ) {
        return new PostgresReplicationListener(
                jdbcUrl,
                databaseUser,
                databasePassword,
                replicationSlotName,
                tablesToListenTo,
                new JsonDeserializingConsumer(consumer)
        );
    }

    /**
     * Create a replication slot if one does not exist.
     */
    void createReplicationSlot();

    /**
     * Drop replication slot. Note - once the replication stream is dropped, any new changes will not be captured.
     * Use with caution. This method should mostly be useful in integration tests.
     */
    void dropReplicationSlot();

    /**
     * Start streaming database changes to the configured consumer.
     */
    void start();

    /**
     * Stop streaming database changes. Once stopped, the ChangeDateCapture instance should not be reused.
     * When used in a service, configure a JVM shutdown hook to call this method and stop the replication stream consumption gracefully.
     */
    void stop();
}
