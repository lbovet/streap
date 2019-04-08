package io.streap.kafka;

import io.streap.test.EmbeddedKafkaSupport;
import io.streap.test.LatchWaiter;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.junit.ClassRule;
import org.junit.Test;
import org.springframework.kafka.test.rule.KafkaEmbedded;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.kafka.receiver.KafkaReceiver;
import reactor.kafka.sender.KafkaSender;
import reactor.kafka.sender.SenderRecord;

import java.util.Collections;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;

import static io.streap.test.EmbeddedKafkaSupport.receiverOptions;
import static io.streap.test.EmbeddedKafkaSupport.senderOptions;
import static org.junit.Assert.assertEquals;

@Slf4j
public class ReactorKafkaTest {

    @ClassRule
    public static KafkaEmbedded embeddedKafka = EmbeddedKafkaSupport.init();

    @Test
    public void testAtLeastOnce() {
        CountDownLatch latch = new CountDownLatch(1);

        Flux.just(
                KafkaReceiver
                        .create(receiverOptions("at.least.once.In"))
                        .receive()
                        .doOnNext(m -> log.info("Received:" + m.value()))
                        .doOnNext(m -> latch.countDown())
                        .doOnError(Throwable::printStackTrace)
                        .subscribe(),

                KafkaSender.create(senderOptions())
                        .send(Mono.just(SenderRecord.create("at.least.once.In", null, null, 1, "hello", 1)))
                        .then()
                        .doOnSuccess(s -> log.info("Sent"))
                        .subscribe()
        )
                .startWith(LatchWaiter.waitOn(latch))
                .doOnNext(Disposable::dispose)
                .blockLast();

        assertEquals(0L, latch.getCount());
    }

    @Test
    public void testExactlyOnce() {
        CountDownLatch latch = new CountDownLatch(1);

        KafkaSender sender = KafkaSender.create(senderOptions()
                .producerProperty(ProducerConfig.TRANSACTIONAL_ID_CONFIG, "SampleTxn"));

        Flux.just(
                KafkaReceiver
                        .create(receiverOptions("exactly.once.In"))
                        .receiveExactlyOnce(sender.transactionManager())
                        .doOnNext(m -> log.info("Received batch:" + m))
                        .concatMap(f -> f)
                        .doOnNext(i -> log.info("Received item:" + i.value()))
                        .doOnNext(i -> latch.countDown())
                        .onErrorResume(e -> sender.transactionManager().abort().then(Mono.error(e)))
                        .subscribe(),

                KafkaSender.create(senderOptions())
                        .send(Mono.just(SenderRecord.create("exactly.once.In", null, null, 1, "hello", 1)))
                        .then()
                        .doOnSuccess(s -> log.info("Sent"))
                        .subscribe()
        )
                .startWith(LatchWaiter.waitOn(latch))
                .doOnNext(Disposable::dispose)
                .blockLast();

        assertEquals(0L, latch.getCount());
    }

    @Test
    public void testConsumeAgain() {
        CountDownLatch latch = new CountDownLatch(3);
        AtomicBoolean shouldStop = new AtomicBoolean(true);
        StringBuffer result = new StringBuffer();

        KafkaSender sender = KafkaSender.create(senderOptions()
                .producerProperty(ProducerConfig.TRANSACTIONAL_ID_CONFIG, "SampleTxn"));

        KafkaReceiver<Integer, String> receiver = KafkaReceiver.create(receiverOptions("consume.again.In"));

        Flux.just(
                receiver
                        .receiveExactlyOnce(sender.transactionManager())
                        .doOnNext(m -> log.info("Received batch"))
                        .concatMap(f -> f
                                .publish().autoConnect()
                                .doOnNext(i -> log.info("Received item:" + i.value()))
                                .doOnNext(i -> {
                                    if (shouldStop.getAndSet(false)) {
                                        log.info("aborting");
                                        throw new RuntimeException("Aborted when item 1 is first seen");
                                    }
                                })
                                .map(ConsumerRecord::value)
                                .doOnNext(result::append)
                                .doOnNext(i -> latch.countDown())
                                .onErrorResume(e ->
                                        receiver.doOnConsumer(consumer -> {
                                            consumer.assignment().forEach((tp) -> {
                                                if (consumer.committed(tp) != null) {
                                                    log.info("reset to " + consumer.committed(tp).offset());
                                                    consumer.seek(tp, consumer.committed(tp).offset());
                                                } else {
                                                    log.info("reset to beginning");
                                                    consumer.seekToBeginning(Collections.singleton(tp));
                                                }
                                            });
                                            return null;
                                        }).then(sender.transactionManager().abort())
                                ))
                        .subscribe(),

                KafkaSender.create(senderOptions())
                        .send(Flux.just("a", "b", "c")
                                .map(x -> SenderRecord.create("consume.again.In", null, null, 1, x, 1)))
                        .then()
                        .doOnSuccess(s -> log.info("Sent"))
                        .subscribe()
        )
                .startWith(LatchWaiter.waitOn(latch))
                .doOnNext(Disposable::dispose)
                .blockLast();

        assertEquals(0L, latch.getCount());
        assertEquals("abc", result.toString());
    }

    @Test
    public void testCache() {
        Flux<Integer> f = Flux
                .just(1, 2, 3, 4, 5, 6)
                .log()
                .cache(1);

        f
                .map(i -> "(" + i + ")")
                .doOnNext(log::info)
                .then(f.last().map(i -> "[" + i + "]"))
                .doOnNext(log::info)
                .block();
    }

    @Test
    public void testError() {
        Flux
                .just(1, 2, 3, 4, 5, 6)
                .doOnNext(i -> {
                    if (i == 3) throw new RuntimeException();
                })
                .then(Mono.just(7))
                .onErrorReturn(8)
                .map(Integer::toUnsignedString)
                .doOnNext(log::info)
                .block();

    }
}
