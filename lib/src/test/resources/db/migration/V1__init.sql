CREATE EXTENSION "uuid-ossp";

CREATE TABLE test_table(
    id UUID NOT NULL PRIMARY KEY,
    integer_field INTEGER,
    text_field TEXT,
    varchar_field VARCHAR(10),
    char_field CHAR(10),
    decimal_field DECIMAL(18, 2),
    bool_field BOOL,
    updated_at TIMESTAMP WITH TIME ZONE NOT NULL
);
