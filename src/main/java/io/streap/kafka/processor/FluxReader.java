package io.streap.kafka.processor;

import io.streap.core.context.Context;
import io.streap.core.processor.StreamProcessor;
import reactor.core.publisher.Flux;
import reactor.kafka.sender.SenderOptions;

import java.util.function.BiFunction;

public class FluxReader<I> extends StreamProcessor<I, Context, Object> {

    private Flux<I> source;

    public FluxReader(Flux<I> source) {
        this.source = source;
    }

    public <K, V> TopicWriter<I, K, V> to(SenderOptions<K, V> senderOptions) {
        return new TopicWriter<>(source, senderOptions);
    }

    @Override
    public Flux<? extends StreamProcessor> process(BiFunction<Flux<? extends I>, Context, Flux<?>> body) {
        return null;
    }
}
