package io.streap.kafka;

import io.streap.kafka.processor.FluxReader;
import io.streap.kafka.processor.TopicReader;
import reactor.core.publisher.Flux;
import reactor.kafka.receiver.ReceiverOptions;

public class KafkaProcessor {

    public static <K, V> TopicReader<K, V> from(ReceiverOptions<K, V> receiverOptions) {
        return new TopicReader<>(receiverOptions);
    }

    public static <I> FluxReader<I> from(Flux<I> source) {
        return new FluxReader<>(source);
    }
}
