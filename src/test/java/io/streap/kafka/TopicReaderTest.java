package io.streap.kafka;

import io.streap.core.block.DefaultBlock;
import io.streap.kafka.processor.KafkaProcessor;
import io.streap.test.EmbeddedKafkaSupport;
import io.streap.test.LatchWaiter;
import io.streap.test.ScopedName;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.BeforeClass;
import org.junit.Test;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.kafka.sender.KafkaSender;
import reactor.kafka.sender.SenderRecord;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;

import static io.streap.test.EmbeddedKafkaSupport.receiverOptions;
import static io.streap.test.EmbeddedKafkaSupport.senderOptions;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@Slf4j
public class TopicReaderTest {

    @BeforeClass
    public static void init() {
        EmbeddedKafkaSupport.init();
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
}
