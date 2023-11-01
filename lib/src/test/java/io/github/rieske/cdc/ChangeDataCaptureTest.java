package io.github.rieske.cdc;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Duration;
import java.time.Instant;
import java.util.Set;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

class ChangeDataCaptureTest {

    @RegisterExtension
    final DatabaseExtension database = new DatabaseExtension();

    private final String replicationSlotName = "cdc_stream";

    private final GatheringConsumer<DatabaseChange> gatheringConsumer = TestConsumers.gathering();

    private final PostgresReplicationListener listener = new PostgresReplicationListener(
            database.jdbcUrl(),
            database.databaseUsername(),
            database.databasePassword(),
            replicationSlotName,
            Set.of("public.test_table"),
            TestConsumers.printing().andThen(new JsonDeserializingConsumer(gatheringConsumer))
    );

    @BeforeEach
    void setup() {
        listener.createReplicationSlot();
        listener.start();
    }

    @AfterEach
    void tearDown() {
        listener.stop();
        listener.dropReplicationSlot();
    }

    @Test
    void capturesInsertEvents() throws SQLException {
        UUID id1 = UUID.randomUUID();
        try (Connection connection = database.getDataSource().getConnection()) {
            insertIntoTestTable(connection, id1, 1,
                    "text1", "varchar1", "char1", new BigDecimal("42"), false, Instant.now());
        }
        UUID id2 = UUID.randomUUID();
        try (Connection connection = database.getDataSource().getConnection()) {
            insertIntoTestTable(connection, id2, 42,
                    "text2", "varchar2", "char2", new BigDecimal("0.42"), true, Instant.now());
        }

        await().atMost(Duration.ofSeconds(2)).untilAsserted(() -> assertThat(gatheringConsumer.consumedMessages).hasSize(2));

        DatabaseChange firstChange = gatheringConsumer.consumedMessages.get(0);
        assertThat(firstChange.action).isEqualTo(DatabaseChange.Action.INSERT);
        assertThat(firstChange.schema).isEqualTo("public");
        assertThat(firstChange.table).isEqualTo("test_table");
        assertThat(firstChange.columns.get("id")).isEqualTo(id1.toString());
        assertThat(firstChange.columns.get("integer_field")).isEqualTo("1");
        assertThat(firstChange.columns.get("text_field")).isEqualTo("text1");
        assertThat(firstChange.columns.get("varchar_field")).isEqualTo("varchar1");
        assertThat(firstChange.columns.get("char_field")).isEqualTo("char1     ");
        assertThat(firstChange.columns.get("decimal_field")).isEqualTo("42.00");
        assertThat(firstChange.columns.get("bool_field")).isEqualTo("false");
        assertThat(firstChange.columns.get("updated_at")).isNotEmpty();

        DatabaseChange secondChange = gatheringConsumer.consumedMessages.get(1);
        assertThat(secondChange.action).isEqualTo(DatabaseChange.Action.INSERT);
        assertThat(secondChange.schema).isEqualTo("public");
        assertThat(secondChange.table).isEqualTo("test_table");
        assertThat(secondChange.columns.get("id")).isEqualTo(id2.toString());
        assertThat(secondChange.columns.get("integer_field")).isEqualTo("42");
        assertThat(secondChange.columns.get("text_field")).isEqualTo("text2");
        assertThat(secondChange.columns.get("varchar_field")).isEqualTo("varchar2");
        assertThat(secondChange.columns.get("char_field")).isEqualTo("char2     ");
        assertThat(secondChange.columns.get("decimal_field")).isEqualTo("0.42");
        assertThat(secondChange.columns.get("bool_field")).isEqualTo("true");
        assertThat(secondChange.columns.get("updated_at")).isNotEmpty();
    }

    @Test
    void capturesUpdateEvents() throws SQLException {
        UUID id = UUID.randomUUID();
        try (Connection connection = database.getDataSource().getConnection()) {
            insertIntoTestTable(connection, id, 1,
                    "text1", "varchar1", "char1", new BigDecimal("42"), false, Instant.now());
        }
        try (Connection connection = database.getDataSource().getConnection()) {
            updateTestTable(connection, id, 1,
                    "text1", "varchar1", null, new BigDecimal("123.45"), true, Instant.now());
        }

        await().atMost(Duration.ofSeconds(2)).untilAsserted(() -> assertThat(gatheringConsumer.consumedMessages).hasSize(2));

        DatabaseChange firstChange = gatheringConsumer.consumedMessages.get(0);
        assertThat(firstChange.action).isEqualTo(DatabaseChange.Action.INSERT);
        assertThat(firstChange.schema).isEqualTo("public");
        assertThat(firstChange.table).isEqualTo("test_table");
        assertThat(firstChange.columns.get("id")).isEqualTo(id.toString());
        assertThat(firstChange.columns.get("integer_field")).isEqualTo("1");
        assertThat(firstChange.columns.get("text_field")).isEqualTo("text1");
        assertThat(firstChange.columns.get("varchar_field")).isEqualTo("varchar1");
        assertThat(firstChange.columns.get("char_field")).isEqualTo("char1     ");
        assertThat(firstChange.columns.get("decimal_field")).isEqualTo("42.00");
        assertThat(firstChange.columns.get("bool_field")).isEqualTo("false");
        assertThat(firstChange.columns.get("updated_at")).isNotEmpty();

        DatabaseChange secondChange = gatheringConsumer.consumedMessages.get(1);
        assertThat(secondChange.action).isEqualTo(DatabaseChange.Action.UPDATE);
        assertThat(secondChange.schema).isEqualTo("public");
        assertThat(secondChange.table).isEqualTo("test_table");
        assertThat(secondChange.columns.get("id")).isEqualTo(id.toString());
        assertThat(secondChange.columns.get("integer_field")).isEqualTo("1");
        assertThat(secondChange.columns.get("text_field")).isEqualTo("text1");
        assertThat(secondChange.columns.get("varchar_field")).isEqualTo("varchar1");
        assertThat(secondChange.columns.get("char_field")).isNull();
        assertThat(secondChange.columns.get("decimal_field")).isEqualTo("123.45");
        assertThat(secondChange.columns.get("bool_field")).isEqualTo("true");
        assertThat(secondChange.columns.get("updated_at")).isNotEmpty();
    }

    @Test
    void listensToCommittedChangesOnly() throws SQLException {
        try (Connection connection = database.getDataSource().getConnection()) {
            connection.setAutoCommit(false);
            insertIntoTestTable(connection, UUID.randomUUID(), 1,
                    "text1", "varchar1", "char1", new BigDecimal("42"), false, Instant.now());
            insertIntoTestTable(connection, UUID.randomUUID(), 42,
                    "text2", "varchar2", "char2", new BigDecimal("0.42"), true, Instant.now());
            connection.rollback();
        }
        await().pollDelay(Duration.ofSeconds(1)).untilAsserted(() -> assertThat(gatheringConsumer.consumedMessages).isEmpty());
    }

    private void insertIntoTestTable(
            Connection connection,
            UUID id,
            int integerField,
            String textField,
            String varcharField,
            String charField,
            BigDecimal decimalField,
            boolean boolField,
            Instant updatedAt
    ) {
        try (PreparedStatement statement = connection.prepareStatement(
                "INSERT INTO test_table" +
                        "(id, integer_field, text_field, varchar_field, char_field, decimal_field, bool_field, updated_at) " +
                        "VALUES (?, ?, ?, ?, ?, ?, ?, ?)")
        ) {
            statement.setObject(1, id);
            statement.setInt(2, integerField);
            statement.setString(3, textField);
            statement.setString(4, varcharField);
            statement.setString(5, charField);
            statement.setBigDecimal(6, decimalField);
            statement.setBoolean(7, boolField);
            statement.setTimestamp(8, Timestamp.from(updatedAt));
            statement.executeUpdate();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    private void updateTestTable(
            Connection connection,
            UUID id,
            int integerField,
            String textField,
            String varcharField,
            String charField,
            BigDecimal decimalField,
            boolean boolField,
            Instant updatedAt
    ) {
        try (PreparedStatement statement = connection.prepareStatement(
                "UPDATE test_table SET " +
                        "integer_field=?, text_field=?, varchar_field=?, char_field=?, " +
                        "decimal_field=?, bool_field=?, updated_at=? " +
                        "WHERE id=?")
        ) {
            statement.setInt(1, integerField);
            statement.setString(2, textField);
            statement.setString(3, varcharField);
            statement.setString(4, charField);
            statement.setBigDecimal(5, decimalField);
            statement.setBoolean(6, boolField);
            statement.setTimestamp(7, Timestamp.from(updatedAt));
            statement.setObject(8, id);
            statement.executeUpdate();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }
}
