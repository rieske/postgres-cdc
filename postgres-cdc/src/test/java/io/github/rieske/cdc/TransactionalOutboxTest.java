package io.github.rieske.cdc;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.time.Duration;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

class TransactionalOutboxTest {

    @RegisterExtension
    final DatabaseExtension database = new DatabaseExtension();

    private final String replicationSlotName = "cdc_stream";
    private final String testEntityOutboxTable = "test_entity_outbox";
    private final String anotherOutboxTable = "another_outbox";

    private final GatheringConsumer<DatabaseChange> gatheringConsumer = new GatheringConsumer<>();

    private final ChangeDataCapture cdc = ChangeDataCapture.create(
            database.jdbcUrl(),
            database.databaseUsername(),
            database.databasePassword(),
            replicationSlotName,
            Set.of("public." + testEntityOutboxTable),
            gatheringConsumer
    );

    @BeforeEach
    void setup() {
        cdc.createReplicationSlot();
        cdc.start();
    }

    @AfterEach
    void tearDown() {
        cdc.stop();
        cdc.dropReplicationSlot();
    }

    @Test
    void capturesInsertEvent() throws SQLException {
        String eventPayload = "{\"foo\":\"bar\"}";
        try (Connection connection = database.getDataSource().getConnection()) {
            insertIntoOutboxTable(connection, testEntityOutboxTable, eventPayload);
        }

        await().atMost(Duration.ofSeconds(2)).untilAsserted(() -> assertThat(gatheringConsumer.consumedMessages).hasSize(1));

        DatabaseChange event = gatheringConsumer.consumedMessages.get(0);
        assertThat(event.action()).isEqualTo(JsonDeserializedDatabaseChange.Action.INSERT);
        assertThat(event.schema()).isEqualTo("public");
        assertThat(event.table()).isEqualTo("test_entity_outbox");
        assertThat(event.columns().get("event_payload")).isEqualTo(eventPayload);
    }

    @Test
    void ignoresEventsFromAnotherTable() throws SQLException {
        try (Connection connection = database.getDataSource().getConnection()) {
            insertIntoOutboxTable(connection, anotherOutboxTable, "{}");
        }
        String eventPayload = "{\"foo\":\"bar\"}";
        try (Connection connection = database.getDataSource().getConnection()) {
            insertIntoOutboxTable(connection, testEntityOutboxTable, eventPayload);
        }

        await().atMost(Duration.ofSeconds(2)).untilAsserted(() -> assertThat(gatheringConsumer.consumedMessages).hasSize(1));

        DatabaseChange event = gatheringConsumer.consumedMessages.get(0);
        assertThat(event.action()).isEqualTo(JsonDeserializedDatabaseChange.Action.INSERT);
        assertThat(event.schema()).isEqualTo("public");
        assertThat(event.table()).isEqualTo("test_entity_outbox");
        assertThat(event.columns().get("event_payload")).isEqualTo(eventPayload);
    }

    private void insertIntoOutboxTable(Connection connection, String outboxTable, String eventPayload) {
        try (PreparedStatement statement = connection.prepareStatement(
                "INSERT INTO " + outboxTable + " (event_payload) VALUES(?::json)"
        )) {
            statement.setString(1, eventPayload);
            statement.executeUpdate();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }
}
