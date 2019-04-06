package io.streap.kafka;

import io.streap.core.block.DefaultBlock;
import io.streap.core.processor.StreamProcessor;
import io.streap.spring.PlatformTransaction;
import io.streap.test.EmbeddedKafkaSupport;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.ClassRule;
import org.junit.Test;
import org.springframework.kafka.test.rule.KafkaEmbedded;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.kafka.receiver.ReceiverOptions;
import reactor.kafka.sender.KafkaSender;
import reactor.kafka.sender.SenderOptions;
import reactor.kafka.sender.SenderRecord;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static io.streap.test.EmbeddedKafkaSupport.*;
import static org.junit.Assert.assertEquals;

public class ProcessorTest {

    @ClassRule
    public static KafkaEmbedded embeddedKafka = EmbeddedKafkaSupport.init();

    @Test
    public void builders() {

        // Idempotent processor
        KafkaProcessor
                .<Long, String>from(ReceiverOptions.create())
                .withIdempotence(null)
                .<Long, String>to(SenderOptions.create())
                .withContext(PlatformTransaction.supplier(null))
                .process((records, context) -> records
                        .flatMap(context.wrapOnce(System.out::println))
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

    @Test
    public void testTopicReaderOk() throws InterruptedException {

        String topic = "test.topic.reader.ok.Name";
        CountDownLatch latch = new CountDownLatch(3);

        KafkaProcessor
                .from(receiverOptions(topic))
                .process((records, context) -> records
                        .map(ConsumerRecord::value)
                        .flatMap(context.wrap(String::toUpperCase))
                        .doOnNext(System.out::println)
                        .doOnNext(x -> latch.countDown()))
                .subscribe();

        // Send the names
        KafkaSender.create(senderOptions())
                .send(Flux.fromArray(new String[] { "paul", "john", "luke"})
                        .map(name -> SenderRecord.create(topic, null, null, 1, name, 1)))
                .then()
                .doOnError(Throwable::printStackTrace)
                .doOnSuccess(s -> System.out.println("Sent"))
                .subscribe();

        latch.await(10, TimeUnit.SECONDS);
        assertEquals(0L, latch.getCount());
    }

    @Test
    public void testTopicReaderFailure() throws InterruptedException {

        String topic = "test.topic.reader.failure.Name";
        CountDownLatch latch = new CountDownLatch(2);

        KafkaProcessor
                .from(receiverOptions(topic))
                .withContext(() -> new DefaultBlock() {
                    @Override
                    public <R> Mono<R> abort() {
                        latch.countDown();
                        return super.abort();
                    }
                })
                .process((records, context) -> records
                        .map(ConsumerRecord::value)
                        .doOnNext(System.out::println)
                        .doOnNext(name -> {
                            if(name.equals("john")) {
                                throw new RuntimeException("Oh");
                            }
                        }))
                .subscribe();

        // Send the names
        KafkaSender.create(senderOptions())
                .send(Flux.fromArray(new String[] { "paul", "john", "luke"})
                        .map(name -> SenderRecord.create(topic, null, null, 1, name, 1)))
                .then()
                .doOnError(Throwable::printStackTrace)
                .doOnSuccess(s -> System.out.println("Sent"))
                .subscribe();

        latch.await(10, TimeUnit.SECONDS);
        assertEquals(0L, latch.getCount());
    }
}


