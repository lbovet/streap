package io.streap.kafka.processor;

import io.streap.core.block.Block;
import io.streap.core.context.Context;
import io.streap.core.idempotence.OffsetStore;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.kafka.receiver.KafkaReceiver;
import reactor.kafka.receiver.ReceiverOptions;
import reactor.kafka.sender.SenderOptions;

import java.time.Duration;
import java.util.function.BiFunction;

public class TopicReader<K, V> extends TopicProcessor<K, V, Context, Object> {

    private ReceiverOptions<K, V> receiverOptions;

    public TopicReader(ReceiverOptions<K, V> receiverOptions) {
        this.receiverOptions = receiverOptions;
    }

    public IdempotentTopicReader<K, V> withIdempotence(OffsetStore offsetStore) {
        return new IdempotentTopicReader<>(receiverOptions, offsetStore);
    }

    public <KP, VP> TopicReaderWriter<K, V, KP, VP> to(SenderOptions<KP, VP> senderOptions) {
        return new TopicReaderWriter<>(receiverOptions, senderOptions);
    }

    @Override
    public Flux<TopicReader> process(BiFunction<Flux<? extends ConsumerRecord<K, V>>, Context, Flux<?>> body) {
        return Flux.just(KafkaReceiver.create(receiverOptions))
                .flatMap(receiver -> receiver
                        .receiveAutoAck()
                        .concatMap(records -> {
                            Block block = createBlock();
                            return body.apply(records, block)
                                    .then(block.commit())
                                    .onErrorResume(abortAndResetOffsets(block.abort(), receiver, receiverOptions));
                        }))
                .thenMany(Mono.<TopicReader>empty())
                .mergeWith(Mono.just(this));
    }
}
