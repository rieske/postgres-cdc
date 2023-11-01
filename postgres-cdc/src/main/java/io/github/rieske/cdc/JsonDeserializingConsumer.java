package io.github.rieske.cdc;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.function.Consumer;

public class JsonDeserializingConsumer implements Consumer<ByteBuffer> {
    private static final ObjectMapper MAPPER = new ObjectMapper()
                .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
                .registerModule(new JavaTimeModule());

    private final Consumer<DatabaseChange> delegate;

    public JsonDeserializingConsumer(Consumer<DatabaseChange> delegate) {
        this.delegate = delegate;
    }

    @Override
    public void accept(ByteBuffer message) {
        int offset = message.arrayOffset();
        byte[] source = message.array();
        int length = source.length - offset;
        try {
            JsonDeserializedDatabaseChange deserializedMessage = MAPPER.readValue(source, offset, length, JsonDeserializedDatabaseChange.class);
            delegate.accept(deserializedMessage);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
