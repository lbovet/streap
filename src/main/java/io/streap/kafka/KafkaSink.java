package io.streap.kafka;

import io.streap.block.Block;
import io.streap.context.Context;
import io.streap.idempotence.OffsetStore;
import io.streap.processor.StreamProcessor;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.kafka.receiver.KafkaReceiver;
import reactor.kafka.receiver.ReceiverOptions;
import reactor.kafka.sender.SenderOptions;

import java.util.function.BiFunction;

public class KafkaSink<K, V> extends StreamProcessor<ConsumerRecord<K, V>, Context, Object> {

    private ReceiverOptions<K, V> receiverOptions;

    public KafkaSink(ReceiverOptions<K, V> receiverOptions) {
        this.receiverOptions = receiverOptions;
    }

    public KafkaIdempotentSink<K, V> withIdempotence(OffsetStore offsetStore) {
        return new KafkaIdempotentSink<>(receiverOptions, offsetStore);
    }

    public <KP, VP> KafkaProcessor<K, V, KP, VP> to(SenderOptions<KP, VP> senderOptions) {
        return new KafkaProcessor<>(receiverOptions, senderOptions);
    }

    @Override
    public Flux<KafkaSink> process(BiFunction<Flux<ConsumerRecord<K, V>>, Context, Flux<Object>> body) {
        return Mono.just(KafkaReceiver.create(receiverOptions))
                .flux()
                .flatMap(receiver -> receiver
                    .receiveAutoAck()
                    .concatMap( records -> {
                        Block block = createBlock();
                        return body.apply(records, block)
                                .then(block.commit())
                                .onErrorResume(e -> block.abort());
                    }))
                .thenMany(Mono.<KafkaSink>empty())
                .mergeWith(Mono.just(this));
    }
}
