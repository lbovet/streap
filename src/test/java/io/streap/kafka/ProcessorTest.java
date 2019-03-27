package io.streap.kafka;

import io.streap.spring.PlatformTransaction;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.Test;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.kafka.receiver.ReceiverOptions;
import reactor.kafka.sender.SenderOptions;

public class ProcessorTest {
    @Test
    public void builders() {

        // Idempotent processor
        KafkaProcessor
                .<Long, String>from(ReceiverOptions.create())
                .withIdempotence(null)
                .<Long, String>to(SenderOptions.create())
                .withContext(PlatformTransaction.supplier(null))
                .process((records, context) -> records
                        .flatMap(context.wrapOnce(x -> {
                        }))
                        .flatMap(context.doOnce(x -> Mono.just(x).then()))
                        .map(ConsumerRecord::value)
                        .map(i -> new ProducerRecord<>("topic.Name", i)));

        // Source
        KafkaProcessor
                .from(Flux.range(1, 2))
                .<String, Integer>to(SenderOptions.create())
                .withContext(PlatformTransaction.supplier(null))
                .process((numbers, context) -> numbers
                        .map(i -> new ProducerRecord<>("topic.Name", i)));
    }
}
