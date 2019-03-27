package io.streap.kafka;

import io.streap.context.Context;
import io.streap.processor.StreamProcessor;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import reactor.core.publisher.Flux;
import reactor.kafka.receiver.ReceiverOptions;
import reactor.kafka.sender.SenderOptions;

import java.util.function.BiFunction;

public class KafkaProcessor<K, V, KP, VP> extends StreamProcessor<ConsumerRecord<K, V>, Context, ProducerRecord<KP, VP>> {

    private ReceiverOptions<K, V> receiverOptions;
    private SenderOptions<KP, VP> senderOptions;

    public KafkaProcessor(ReceiverOptions<K, V> receiverOptions, SenderOptions<KP, VP> senderOptions) {
        this.receiverOptions = receiverOptions;
        this.senderOptions = senderOptions;
    }

    public static <K, V> KafkaSink<K, V> from(ReceiverOptions<K, V> receiverOptions) {
        return new KafkaSink<>(receiverOptions);
    }

    public static <I> KafkaFluxProcessor<I> from(Flux<I> source) {
        return new KafkaFluxProcessor<>(source);
    }

    @Override
    public Flux<? extends StreamProcessor> process(BiFunction<Flux<ConsumerRecord<K, V>>, Context, Flux<ProducerRecord<KP, VP>>> body) {
        return null;
    }
}
