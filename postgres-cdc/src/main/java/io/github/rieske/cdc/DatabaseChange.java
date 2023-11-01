package io.github.rieske.cdc;

import java.util.Map;

/**
 * A record, representing a change in the database.
 * Exposes the action (INSERT/UPDATE/DELETE/TRUNCATE), schema, table, and a map of column names and their values, all as Strings.
 */
public interface DatabaseChange {

    /**
     * @return the INSERT/UPDATE/DELETE/TRUNCATE action that yielded this change.
     */
    Action action();

    /**
     * @return the schema where this change originated.
     */
    String schema();

    /**
     * @return the table where this change originated.
     */
    String table();

    /**
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
