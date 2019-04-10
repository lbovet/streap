package io.streap.kafka;

import io.streap.kafka.processor.KafkaProcessor;
import io.streap.test.EmbeddedKafkaSupport;
import io.streap.test.LatchWaiter;
import io.streap.test.ScopedName;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.BeforeClass;
import org.junit.Test;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;
import reactor.kafka.receiver.KafkaReceiver;
import reactor.kafka.sender.KafkaSender;
import reactor.kafka.sender.SenderRecord;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

import static io.streap.test.EmbeddedKafkaSupport.receiverOptions;
import static io.streap.test.EmbeddedKafkaSupport.senderOptions;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@Slf4j
public class TopicReaderWriterTest {

    @BeforeClass
    public static void init() {
        EmbeddedKafkaSupport.init();
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
