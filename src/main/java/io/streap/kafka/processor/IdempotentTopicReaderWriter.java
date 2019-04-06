package io.streap.kafka.processor;

import io.streap.core.idempotence.IdempotentContext;
import io.streap.core.idempotence.OffsetStore;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import reactor.core.publisher.Flux;
import reactor.kafka.receiver.ReceiverOptions;
import reactor.kafka.sender.SenderOptions;

import java.util.function.BiFunction;

public class IdempotentTopicReaderWriter<K, V, KP, VP> extends ReceivingProcessor<K, V, IdempotentContext<ConsumerRecord<K, V>>, ProducerRecord<KP, VP>> {

    private ReceiverOptions<K, V> receiverOptions;
    private OffsetStore offsetStore;
    private SenderOptions<KP, VP> senderOptions;

    public IdempotentTopicReaderWriter(ReceiverOptions<K, V> receiverOptions, OffsetStore offsetStore, SenderOptions<KP, VP> senderOptions) {
        this.receiverOptions = receiverOptions;
        this.offsetStore = offsetStore;
        this.senderOptions = senderOptions;
    }

    @Override
    public <T extends ProducerRecord<KP, VP>> Flux<T> process(BiFunction<Flux<? extends ConsumerRecord<K, V>>, IdempotentContext<ConsumerRecord<K, V>>, Flux<T>> body) {
        return null;
    }
}
