package io.streap.kafka.processor;

import io.streap.core.context.Context;
import io.streap.core.processor.StreamProcessor;
import org.apache.kafka.clients.producer.ProducerRecord;
import reactor.core.publisher.Flux;
import reactor.kafka.sender.SenderOptions;

import java.util.function.BiFunction;

public class TopicWriter<I, K, V> extends StreamProcessor<I, Context, ProducerRecord<K, V>> {
    private Flux<I> source;
    private SenderOptions<K, V> senderOptions;

    public TopicWriter(Flux<I> source, SenderOptions<K, V> senderOptions) {
        this.source = source;
        this.senderOptions = senderOptions;
    }

    @Override
    public <T extends ProducerRecord<K, V>> Flux<T> process(BiFunction<Flux<? extends I>, Context, Flux<T>> body) {
        return null;
    }
}
