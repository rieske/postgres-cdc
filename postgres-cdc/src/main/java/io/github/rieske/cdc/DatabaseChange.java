package io.github.rieske.cdc;

import java.util.Map;

/**
 * A record, representing a change in the database.
 * Exposes the action (INSERT/UPDATE/DELETE/TRUNCATE), schema, table, and a map of column names and their values, all as Strings.
 */
public interface DatabaseChange {

    /**
     * The INSERT/UPDATE/DELETE/TRUNCATE action that yielded this change.
     *
     * @return the action name.
     */
    Action action();

    /**
     * The schema where this change originated.
     *
     * @return the schema name.
     */
    String schema();

    /**
     * The table where this change originated.
     *
     * @return the table name.
     */
    String table();

    /**
     * Column names and their values as from the database change.
     *
     * @return a Map of column names and their values as Strings from the database change.
     *  Contains all columns from the changed table - both changed and unchanged.
     */
    Map<String, String> columns();

    /**
     * An action that was performed on the database to cause a change.
     */
    enum Action {

        /**
         * Indicates that the database change was created using INSERT command
         */
        INSERT,

        /**
         * Indicates that the database change was created using UPDATE command
         */
        UPDATE,

        /**
         * Indicates that the database change was created using DELETE command
         */
        DELETE,

        /**
         * Indicates that the database change was created using TRUNCATE command
         */
        TRUNCATE
    }
}
