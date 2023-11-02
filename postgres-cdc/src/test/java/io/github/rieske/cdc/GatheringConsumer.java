package io.github.rieske.cdc;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.Consumer;

class GatheringConsumer<T> implements Consumer<T> {
    final List<T> consumedMessages = new CopyOnWriteArrayList<>();

    @Override
    public void accept(T message) {
        consumedMessages.add(message);
    }
}
