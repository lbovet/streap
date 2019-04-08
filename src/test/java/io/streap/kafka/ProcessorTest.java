package io.streap.kafka;

import io.streap.core.block.DefaultBlock;
import io.streap.spring.PlatformTransaction;
import io.streap.test.EmbeddedKafkaSupport;
import io.streap.test.LatchWaiter;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.ClassRule;
import org.junit.Test;
import org.springframework.kafka.test.rule.KafkaEmbedded;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.kafka.receiver.KafkaReceiver;
import reactor.kafka.receiver.ReceiverOptions;
import reactor.kafka.sender.KafkaSender;
import reactor.kafka.sender.SenderOptions;
import reactor.kafka.sender.SenderRecord;

import java.util.concurrent.CountDownLatch;

import static io.streap.test.EmbeddedKafkaSupport.receiverOptions;
import static io.streap.test.EmbeddedKafkaSupport.senderOptions;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@Slf4j
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
    public void testTopicReaderOk() {

        String topic = "test.topic.reader.ok";
        CountDownLatch latch = new CountDownLatch(3);
        StringBuilder result = new StringBuilder();

        Flux.just(
                KafkaProcessor
                        .from(receiverOptions(topic))
                        .process((records, context) -> records
                                .map(ConsumerRecord::value)
                                .flatMap(context.wrap(String::toUpperCase))
                                .log("testTopicReaderOk")
                                .doOnNext(result::append)
                                .doOnNext(x -> latch.countDown()))
                        .subscribe(),

                KafkaSender.create(senderOptions())
                        .send(Flux.just("A", "B", "C")
                                .map(n -> SenderRecord.create(topic, null, null, 1, n, 1)))
                        .then()
                        .doOnSuccess(s -> log.info("Sent"))
                        .subscribe()
        )
                .startWith(LatchWaiter.waitOn(latch))
                .doOnNext(Disposable::dispose)
                .blockLast();

        assertEquals(0L, latch.getCount());
        assertTrue(result.toString().startsWith("ABC"));
    }

    @Test
    public void testTopicReaderFailure() {

        String topic = "test.topic.reader.failure";
        CountDownLatch latch = new CountDownLatch(3);
        StringBuilder result = new StringBuilder();

        Flux.just(
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
                                .flatMap(context.wrap(String::toUpperCase))
                                .log("testTopicReaderFailure")
                                .doOnNext(result::append)
                                .doOnNext(n -> {
                                    if (n.equals("B")) {
                                        throw new RuntimeException("Oh");
                                    }
                                }))
                        .subscribe(),

                KafkaSender.create(senderOptions())
                        .send(Flux.just("a", "b", "c")
                                .map(n -> SenderRecord.create(topic, null, null, 1, n, 1)))
                        .then()
                        .doOnSuccess(s -> log.info("Sent"))
                        .subscribe()
        )
                .startWith(LatchWaiter.waitOn(latch))
                .doOnNext(Disposable::dispose)
                .blockLast();

        assertEquals(0L, latch.getCount());
        assertTrue(result.toString().startsWith("ABAB"));
    }

    @Test
    public void testTopicReaderWriterOk() {
        String topicIn = "test.topic.reader.writer.ok.input";
        String topicOut = topicIn.replace("input", "output");
        CountDownLatch latch = new CountDownLatch(3);
        StringBuilder result = new StringBuilder();

        Flux.just(
                KafkaProcessor
                        .from(receiverOptions(topicIn))
                        .to(senderOptions("txId"))
                        .process((records, context) -> records
                                .map(ConsumerRecord::value)
                                .flatMap(context.wrap(String::toUpperCase))
                                .map(name -> new ProducerRecord<>(topicOut, name)))
                        .subscribe(),

                KafkaSender.create(senderOptions())
                        .send(Flux.just("a", "b", "c")
                                .map(n -> SenderRecord.create(topicIn, null, null, 1, n, 1)))
                        .then()
                        .doOnSuccess(s -> log.info("Sent"))
                        .subscribe(),

                KafkaReceiver
                        .create(receiverOptions(topicOut))
                        .receiveAutoAck()
                        .concatMap(x -> x)
                        .map(ConsumerRecord::value)
                        .doOnNext(result::append)
                        .log("testTopicReaderWriterOk")
                        .doOnNext(m -> latch.countDown())
                        .subscribe()
        )
                .startWith(LatchWaiter.waitOn(latch))
                .doOnNext(Disposable::dispose)
                .blockLast();

        assertEquals(0L, latch.getCount());
        assertEquals("ABC", result.toString());
    }

}


