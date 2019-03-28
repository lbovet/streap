package io.streap.kafka;

import io.streap.idempotence.IdempotentContext;
import io.streap.idempotence.OffsetStore;
import io.streap.processor.StreamProcessor;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import reactor.core.publisher.Flux;
import reactor.kafka.receiver.ReceiverOptions;
import reactor.kafka.sender.SenderOptions;

import java.util.function.BiFunction;

public class KafkaIdempotentSink<K, V> extends StreamProcessor<ConsumerRecord<K, V>, IdempotentContext<ConsumerRecord<K, V>>, Object> {

    private ReceiverOptions<K, V> receiverOptions;
    private OffsetStore offsetStore;

    public KafkaIdempotentSink(ReceiverOptions<K, V> receiverOptions, OffsetStore offsetStore) {
        this.receiverOptions = receiverOptions;
        this.offsetStore = offsetStore;
    }

    public <KP, VP> KafkaIdempotentProcessor<K, V, KP, VP> to(SenderOptions<KP, VP> senderOptions) {
        return new KafkaIdempotentProcessor<>(receiverOptions, offsetStore, senderOptions);
    }

    @Override
    public Flux<? extends StreamProcessor> process(BiFunction<Flux<ConsumerRecord<K, V>>, IdempotentContext<ConsumerRecord<K, V>>, Flux<Object>> body) {
        return null;
    }
}
