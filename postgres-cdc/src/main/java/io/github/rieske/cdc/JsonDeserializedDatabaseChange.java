package io.github.rieske.cdc;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

class JsonDeserializedDatabaseChange implements DatabaseChange {
    private final Action action;
    private final String schema;
    private final String table;
    private final Map<String, String> columns;

    @JsonCreator
    JsonDeserializedDatabaseChange(
            @JsonProperty("action") String action,
            @JsonProperty("schema") String schema,
            @JsonProperty("table") String table,
            @JsonProperty("columns") List<Column> columns
    ) {
        switch (action) {
            case "I":
                this.action = Action.INSERT;
                break;
            case "U":
                this.action = Action.UPDATE;
                break;
            case "D":
                this.action = Action.DELETE;
                break;
            case "T":
                this.action = Action.TRUNCATE;
                break;
            default:
                throw new IllegalArgumentException("Unrecognized database change action: " + action);
        }
        this.schema = schema;
        this.table = table;
        Map<String, String> mutableColumns = new HashMap<>();
        for (Column column : columns) {
            mutableColumns.put(column.name, column.value);
        }
        this.columns = Collections.unmodifiableMap(mutableColumns);
    }

    @Override
    public String toString() {
        return "DatabaseChange{" +
                "action='" + action + '\'' +
                ", schema='" + schema + '\'' +
                ", table='" + table + '\'' +
                ", columns=" + columns +
                '}';
    }

    @Override
    public Action action() {
        return action;
    }

    @Override
    public String schema() {
        return schema;
    }

    @Override
    public String table() {
        return table;
    }

    @Override
    public Map<String, String> columns() {
        return columns;
    }

    static class Column {
        private final String name;
        private final String value;

        @JsonCreator
        Column(@JsonProperty("name") String name, @JsonProperty("value") String value) {
            this.name = name;
            this.value = value;
        }
    }
}
