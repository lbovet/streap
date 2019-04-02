package io.streap.kafka.processor;

import io.streap.core.block.Block;
import io.streap.core.context.Context;
import io.streap.core.idempotence.OffsetStore;
import io.streap.core.processor.StreamProcessor;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.kafka.receiver.KafkaReceiver;
import reactor.kafka.receiver.ReceiverOptions;
import reactor.kafka.sender.SenderOptions;

import java.time.Duration;
import java.util.Optional;
import java.util.function.BiFunction;

public class TopicReader<K, V> extends StreamProcessor<ConsumerRecord<K, V>, Context, Object> {

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

    /**
     * TODO:
     */
    @Override
    public Flux<TopicReader> process(BiFunction<Flux<ConsumerRecord<K, V>>, Context, Flux<?>> body) {
        /*int fetchMaxWait =
                (int)receiverOptions.consumerProperties().getOrDefault(ConsumerConfig.F, 500);
        int fetchMinSize =
                (int)receiverOptions.consumerProperties().getOrDefault(ConsumerConfig.FE, 500);

        Duration.ofMillis();
        Long fetchMaxWait = Duration.ofMillis(receiverOptions.consumerProperty(ConsumerConfig.FETCH_MAX_WAIT_MS_CONFIG));*/
        return Mono.just(KafkaReceiver.create(receiverOptions))
                .flux()
                .flatMap(receiver -> receiver
                        .receive()
                        //.windowTimeout(fetchMaxWait, fetchMinSize)
                        .concatMap(records -> {
                            Block block = createBlock();
                            return body.apply(records, block)
                                    .then(block.commit())
                                    .onErrorResume(e -> block.abort());
                        }))
                .thenMany(Mono.<TopicReader>empty())
                .mergeWith(Mono.just(this));
    }
}
