package io.github.rieske.cdc;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

class DatabaseChange {
    public final Action action;
    public final String schema;
    public final String table;
    public final Map<String, String> columns;

    @JsonCreator
    DatabaseChange(
            @JsonProperty("action") Action action,
            @JsonProperty("schema") String schema,
            @JsonProperty("table") String table,
            @JsonProperty("columns") List<Column> columns
    ) {
        this.action = action;
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

    enum Action {
        @JsonProperty("I")
        INSERT,
        @JsonProperty("U")
        UPDATE,
        @JsonProperty("D")
        DELETE,
        @JsonProperty("T")
        TRUNCATE
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
