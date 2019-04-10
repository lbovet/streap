package io.streap.kafka;

import io.streap.core.block.DefaultBlock;
import io.streap.spring.PlatformTransaction;
import io.streap.test.EmbeddedKafkaSupport;
import io.streap.test.LatchWaiter;
import io.streap.test.ScopedName;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.kafka.receiver.KafkaReceiver;
import reactor.kafka.receiver.ReceiverOptions;
import reactor.kafka.sender.KafkaSender;
import reactor.kafka.sender.SenderOptions;
import reactor.kafka.sender.SenderRecord;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static io.streap.test.EmbeddedKafkaSupport.receiverOptions;
import static io.streap.test.EmbeddedKafkaSupport.senderOptions;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@Slf4j
public class ProcessorTest {

    @BeforeClass
    public static void init() {
        EmbeddedKafkaSupport.init();
    }

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

        String topic = ScopedName.get();
        CountDownLatch latch = new CountDownLatch(3);
        StringBuilder result = new StringBuilder();

        Flux.just(
                KafkaProcessor
                        .from(receiverOptions(topic))
                        .process((records, context) -> records
                                .map(ConsumerRecord::value)
                                .flatMap(context.wrap(String::toUpperCase))
                                .log(ScopedName.get())
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
        String topic = ScopedName.get();
        CountDownLatch latch = new CountDownLatch(2);
        StringBuilder result = new StringBuilder();
        AtomicBoolean shouldFail = new AtomicBoolean(true);

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
                                .log(ScopedName.get())
                                .doOnNext(n -> {
                                    if (n.equals("B") && shouldFail.get()) {
                                        shouldFail.set(false);
                                        throw new RuntimeException("Oh");
                                    }
                                })
                                .doOnNext(result::append)
                                .doOnNext(n -> {
                                    if (n.equals("C")) {
                                        latch.countDown();
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
        log.info(result.toString());
        assertTrue(result.toString().startsWith("A"));
        assertTrue(result.toString().contains("B"));
        assertTrue(result.toString().endsWith("C"));
    }

    @Test
    public void testTopicReaderWriterOk() {
        String topicIn = ScopedName.get("input");
        String topicOut = ScopedName.get("output");
        CountDownLatch latch = new CountDownLatch(3);
        StringBuilder result = new StringBuilder();

        Flux.just(
                KafkaProcessor
                        .from(receiverOptions(topicIn))
                        .to(senderOptions(ScopedName.get()))
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
                        .log(ScopedName.get())
                        .doOnNext(m -> latch.countDown())
                        .subscribe()
        )
                .startWith(LatchWaiter.waitOn(latch))
                .doOnNext(Disposable::dispose)
                .blockLast();

        assertEquals(0L, latch.getCount());
        assertEquals("ABC", result.toString());
    }

    @Test
    public void testTopicReaderWriterFailure() {
        String topicIn = ScopedName.get("input");
        String topicOut = ScopedName.get("output");
        CountDownLatch latch = new CountDownLatch(2);
        StringBuilder intermediateResult = new StringBuilder();
        StringBuilder finalResult = new StringBuilder();
        AtomicInteger batch = new AtomicInteger(0);

        Flux.just(
                KafkaProcessor
                        .from(receiverOptions(topicIn).consumerProperty(ConsumerConfig.GROUP_ID_CONFIG, ScopedName.get()))
                        .to(senderOptions(ScopedName.get()))
                        .process((records, context) -> {
                            log.info("Batch {}", batch.incrementAndGet());
                            return records
                                    .map(ConsumerRecord::value)
                                    .map(n -> n + batch)
                                    .log()
                                    .doOnNext(intermediateResult::append)
                                    .doOnNext(n -> {
                                        if (n.equals("a1")) {
                                            throw new RuntimeException("Ouch");
                                        }
                                    })
                                    .doOnNext(n -> {
                                        if (n.equals("b2")) {
                                            latch.countDown();
                                            throw new RuntimeException("Ouch");
                                        }
                                    })
                                    .flatMap(context.wrap(String::toUpperCase))
                                    .map(name -> new ProducerRecord<>(topicOut, name));
                        })
                        .subscribe(),

                KafkaSender.create(senderOptions())
                        .send(Flux.just("a", "b", "c")
                                .map(n -> SenderRecord.create(topicIn, null, null, 1, n, 1)))
                        .then()
                        .doOnSuccess(s -> log.info("Sent"))
                        .doOnSuccess(s -> latch.countDown())
                        .subscribe()
        )
                .startWith(LatchWaiter.waitOn(latch))
                .doOnNext(Disposable::dispose)
                .blockLast();

        assertEquals(0L, latch.getCount());
        log.info(intermediateResult.toString());
        assertTrue(intermediateResult.toString().startsWith("a1a2b2"));

        CountDownLatch latch2 = new CountDownLatch(3);

        Flux.just(
                KafkaProcessor
                        .from(receiverOptions(topicIn).consumerProperty(ConsumerConfig.GROUP_ID_CONFIG, ScopedName.get()))
                        .to(senderOptions(ScopedName.get()))
                        .process((records, context) -> records
                                .map(ConsumerRecord::value)
                                .flatMap(context.wrap(String::toUpperCase))
                                .map(name -> new ProducerRecord<>(topicOut, name)))
                        .subscribe(),

                KafkaReceiver
                        .create(receiverOptions(topicOut))
                        .receiveAutoAck()
                        .concatMap(x -> x)
                        .map(ConsumerRecord::value)
                        .doOnNext(finalResult::append)
                        .log(ScopedName.get())
                        .doOnNext(m -> latch2.countDown())
                        .subscribe()
        )
                .startWith(LatchWaiter.waitOn(latch2))
                .doOnNext(Disposable::dispose)
                .blockLast();


        assertEquals(0L, latch2.getCount());
        assertEquals("ABC", finalResult.toString());
    }

}


