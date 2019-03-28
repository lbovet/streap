package io.streap.kafka;

import io.streap.context.Context;
import io.streap.processor.StreamProcessor;
import reactor.core.publisher.Flux;
import reactor.kafka.sender.SenderOptions;

import java.util.function.BiFunction;

public class KafkaFluxProcessor<I> extends StreamProcessor<I, Context, Object> {

    private Flux<I> source;

    public KafkaFluxProcessor(Flux<I> source) {
        this.source = source;
    }

    public <K, V> KafkaSource<I, K, V> to(SenderOptions<K, V> senderOptions) {
        return new KafkaSource<>(source, senderOptions);
    }

    @Override
    public Flux<? extends StreamProcessor> process(BiFunction<Flux<I>, Context, Flux<Object>> body) {
        return null;
    }
}
