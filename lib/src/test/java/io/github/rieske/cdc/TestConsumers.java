package io.github.rieske.cdc;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.Consumer;

final class TestConsumers {
    private TestConsumers() {
    }

    static Consumer<ByteBuffer> printing() {
        return msg -> {
            int offset = msg.arrayOffset();
            byte[] source = msg.array();
            int length = source.length - offset;
            System.out.println(new String(source, offset, length));
        };
    }

    static <T> GatheringConsumer<T> gathering() {
        return new GatheringConsumer<>();
    }
}

class GatheringConsumer<T> implements Consumer<T> {
    final List<T> consumedMessages = new CopyOnWriteArrayList<>();

    @Override
    public void accept(T message) {
        consumedMessages.add(message);
    }
}
