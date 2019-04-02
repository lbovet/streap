package io.streap.kafka.processor;

import io.streap.core.idempotence.IdempotentContext;
import io.streap.core.idempotence.OffsetStore;
import io.streap.core.processor.StreamProcessor;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import reactor.core.publisher.Flux;
import reactor.kafka.receiver.ReceiverOptions;
import reactor.kafka.sender.SenderOptions;

import java.util.function.BiFunction;

public class IdempotentTopicReaderWriter<K, V, KP, VP> extends StreamProcessor<ConsumerRecord<K, V>, IdempotentContext<ConsumerRecord<K, V>>, ProducerRecord<KP, VP>> {

    private ReceiverOptions<K, V> receiverOptions;
    private OffsetStore offsetStore;
    private SenderOptions<KP, VP> senderOptions;

    public IdempotentTopicReaderWriter(ReceiverOptions<K, V> receiverOptions, OffsetStore offsetStore, SenderOptions<KP, VP> senderOptions) {
        this.receiverOptions = receiverOptions;
        this.offsetStore = offsetStore;
        this.senderOptions = senderOptions;
    }

    @Override
    public Flux<? extends StreamProcessor> process(BiFunction<Flux<ConsumerRecord<K, V>>, IdempotentContext<ConsumerRecord<K, V>>, Flux<? extends ProducerRecord<KP, VP>>> body) {
        return null;
    }
}
