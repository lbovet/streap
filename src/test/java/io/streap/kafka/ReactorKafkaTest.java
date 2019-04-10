package io.streap.kafka;

import io.streap.test.EmbeddedKafkaSupport;
import io.streap.test.LatchWaiter;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.junit.BeforeClass;
import org.junit.Test;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.kafka.receiver.KafkaReceiver;
import reactor.kafka.sender.KafkaSender;
import reactor.kafka.sender.SenderRecord;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

import static io.streap.test.EmbeddedKafkaSupport.receiverOptions;
import static io.streap.test.EmbeddedKafkaSupport.senderOptions;
import static org.junit.Assert.assertEquals;

/**
 * Confirms that the behaviour of underlying libraries keeps the same across versions.
 */
@Slf4j
public class ReactorKafkaTest {

    @BeforeClass
    public static void init() {
        EmbeddedKafkaSupport.init();
    }

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

    @Test
    public void testDrain() {
        AtomicInteger last = new AtomicInteger();
        Flux.range(0, 10000).doOnNext(last::set).publish().autoConnect().take(100).blockLast();
        assertEquals(9999, last.get());
    }
}
