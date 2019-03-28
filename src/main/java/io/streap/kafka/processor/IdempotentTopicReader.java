package io.streap.kafka.processor;

import io.streap.core.idempotence.IdempotentContext;
import io.streap.core.idempotence.OffsetStore;
import io.streap.core.processor.StreamProcessor;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import reactor.core.publisher.Flux;
import reactor.kafka.receiver.ReceiverOptions;
import reactor.kafka.sender.SenderOptions;

import java.util.function.BiFunction;

public class IdempotentTopicReader<K, V> extends StreamProcessor<ConsumerRecord<K, V>, IdempotentContext<ConsumerRecord<K, V>>, Object> {

    private ReceiverOptions<K, V> receiverOptions;
    private OffsetStore offsetStore;

    public IdempotentTopicReader(ReceiverOptions<K, V> receiverOptions, OffsetStore offsetStore) {
        this.receiverOptions = receiverOptions;
        this.offsetStore = offsetStore;
    }

    public <KP, VP> IdempotentTopicReaderWriter<K, V, KP, VP> to(SenderOptions<KP, VP> senderOptions) {
        return new IdempotentTopicReaderWriter<>(receiverOptions, offsetStore, senderOptions);
    }

    @Override
    public Flux<? extends StreamProcessor> process(BiFunction<Flux<ConsumerRecord<K, V>>, IdempotentContext<ConsumerRecord<K, V>>, Flux<Object>> body) {
        return null;
    }
}
