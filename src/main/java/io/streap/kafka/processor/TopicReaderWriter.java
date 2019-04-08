package io.streap.kafka.processor;

import io.streap.core.block.Block;
import io.streap.core.context.Context;
import io.streap.kafka.block.ExactlyOnceBlock;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;
import reactor.kafka.receiver.KafkaReceiver;
import reactor.kafka.receiver.ReceiverOptions;
import reactor.kafka.sender.KafkaSender;
import reactor.kafka.sender.SenderOptions;
import reactor.kafka.sender.SenderRecord;
import reactor.kafka.sender.TransactionManager;
import reactor.util.function.Tuple2;

import java.util.function.BiFunction;

public class TopicReaderWriter<K, V, KP, VP> extends ReceivingProcessor<K, V, Context, ProducerRecord<KP, VP>> {

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
                                return body.apply(records.publish().autoConnect(), block)
                                                .log()
                                        .transform( f -> f
                                                .zipWith(f
                                                        .publishOn(Schedulers.parallel())
                                                        .map(p -> SenderRecord.create(p, null))
                                                        .compose(sender::send)))
                                        .map(Tuple2::getT1)
                                        .then(block.commit())
                                        .onErrorResume(abortAndResetOffsets(block.abort(), receiver, receiverOptions))
                                        .then(Mono.empty());
                            });
                });
    }
}
