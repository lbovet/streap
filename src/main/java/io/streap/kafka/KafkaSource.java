package io.streap.kafka;

import io.streap.context.Context;
import io.streap.processor.StreamProcessor;
import org.apache.kafka.clients.producer.ProducerRecord;
import reactor.core.publisher.Flux;
import reactor.kafka.sender.SenderOptions;

import java.util.function.BiFunction;

public class KafkaSource<I, K, V> extends StreamProcessor<I, Context, ProducerRecord<K, V>> {
    private Flux<I> source;
    private SenderOptions<K, V> senderOptions;

    public KafkaSource(Flux<I> source, SenderOptions<K, V> senderOptions) {
        this.source = source;
        this.senderOptions = senderOptions;
    }

    @Override
    public Flux<? extends StreamProcessor> process(BiFunction<Flux<I>, Context, Flux<ProducerRecord<K, V>>> body) {
        return null;
    }
}
