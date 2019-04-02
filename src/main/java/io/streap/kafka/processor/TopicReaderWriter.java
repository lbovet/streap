package io.streap.kafka.processor;

import io.streap.core.context.Context;
import io.streap.core.processor.StreamProcessor;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import reactor.core.publisher.Flux;
import reactor.kafka.receiver.ReceiverOptions;
import reactor.kafka.sender.SenderOptions;

import java.util.function.BiFunction;

public class TopicReaderWriter<K, V, KP, VP> extends StreamProcessor<ConsumerRecord<K, V>, Context, ProducerRecord<KP, VP>> {

    private ReceiverOptions<K, V> receiverOptions;
    private SenderOptions<KP, VP> senderOptions;

    public TopicReaderWriter(ReceiverOptions<K, V> receiverOptions, SenderOptions<KP, VP> senderOptions) {
        this.receiverOptions = receiverOptions;
        this.senderOptions = senderOptions;
    }

    @Override
    public Flux<? extends StreamProcessor> process(BiFunction<Flux<ConsumerRecord<K, V>>, Context, Flux<? extends ProducerRecord<KP, VP>>> body) {
        return null;
    }
}
