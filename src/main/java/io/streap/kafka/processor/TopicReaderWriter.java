package io.streap.kafka.processor;

import io.streap.core.block.Block;
import io.streap.core.context.Context;
import io.streap.core.processor.StreamProcessor;
import io.streap.kafka.block.ExactlyOnceBlock;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.kafka.receiver.KafkaReceiver;
import reactor.kafka.receiver.ReceiverOptions;
import reactor.kafka.sender.*;

import java.util.function.BiFunction;

public class TopicReaderWriter<K, V, KP, VP> extends StreamProcessor<ConsumerRecord<K, V>, Context, ProducerRecord<KP, VP>> {

    private ReceiverOptions<K, V> receiverOptions;
    private SenderOptions<KP, VP> senderOptions;

    public TopicReaderWriter(ReceiverOptions<K, V> receiverOptions, SenderOptions<KP, VP> senderOptions) {
        this.receiverOptions = receiverOptions;
        this.senderOptions = senderOptions;
    }

    @Override
    public <T extends ProducerRecord<KP, VP>> Flux<T> process(BiFunction<Flux<? extends ConsumerRecord<K, V>>, Context, Flux<T>> body) {

        return Flux.just(KafkaReceiver.create(receiverOptions))
                .flatMap(receiver -> {
                    KafkaSender<KP, VP> sender = KafkaSender.create(senderOptions.stopOnError(true));
                    TransactionManager transactionManager = sender.transactionManager();
                    return receiver
                            .receiveExactlyOnce(transactionManager)
                            .concatMap(records -> {
                                Block block = new ExactlyOnceBlock<>(transactionManager, createBlock(), records);
                                return body.apply(records, block)
                                        .map(p -> SenderRecord.create(p, p))
                                        .compose(sender::send)
                                        .map(SenderResult::correlationMetadata)
                                        .concatWith(block.commit())
                                        .onErrorResume(e -> block.abort().then(Mono.error(e)));
                            });
                })
                .compose(ErrorHandler.retry(receiverOptions));
    }
}
