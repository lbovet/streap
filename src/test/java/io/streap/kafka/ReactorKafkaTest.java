package io.streap.kafka;

import io.streap.test.EmbeddedKafkaSupport;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.junit.ClassRule;
import org.junit.Test;
import org.springframework.kafka.test.rule.KafkaEmbedded;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.kafka.receiver.KafkaReceiver;
import reactor.kafka.sender.KafkaSender;
import reactor.kafka.sender.SenderRecord;

import java.util.Arrays;
import java.util.Collections;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Stream;

import static io.streap.test.EmbeddedKafkaSupport.receiverOptions;
import static io.streap.test.EmbeddedKafkaSupport.senderOptions;
import static org.junit.Assert.assertEquals;


public class ReactorKafkaTest {

    @ClassRule
    public static KafkaEmbedded embeddedKafka = EmbeddedKafkaSupport.init();

    @Test
    public void testAtLeastOnce() throws InterruptedException {
        CountDownLatch latch = new CountDownLatch(1);

        KafkaReceiver
                .create(receiverOptions("at.least.once.In"))
                .receive()
                .doOnNext(m -> System.out.println("Received:" + m.value()))
                .doOnNext(m -> latch.countDown())
                .doOnError(Throwable::printStackTrace)
                .subscribe();

        Thread.sleep(500);

        KafkaSender.create(senderOptions())
                .send(Mono.just(SenderRecord.create("at.least.once.In", null, null, 1, "hello", 1)))
                .then()
                .doOnError(Throwable::printStackTrace)
                .doOnSuccess(s -> System.out.println("Sent"))
                .subscribe();


        latch.await(5, TimeUnit.SECONDS);
        assertEquals(0L, latch.getCount());
    }

    @Test
    public void testExactlyOnce() throws InterruptedException {
        CountDownLatch latch = new CountDownLatch(1);

        KafkaSender sender = KafkaSender.create(senderOptions()
                .producerProperty(ProducerConfig.TRANSACTIONAL_ID_CONFIG, "SampleTxn"));

        KafkaReceiver
                .create(receiverOptions("exactly.once.In"))
                .receiveExactlyOnce(sender.transactionManager())
                .doOnNext(m -> System.out.println("Received batch:" + m))
                .concatMap(f -> f)
                .doOnNext(i -> System.out.println("Received item:" + i.value()))
                .doOnNext(i -> latch.countDown())
                .onErrorResume(e -> sender.transactionManager().abort().then(Mono.error(e)))
                .subscribe();

        Thread.sleep(500);

        KafkaSender.create(senderOptions())
                .send(Mono.just(SenderRecord.create("exactly.once.In", null, null, 1, "hello", 1)))
                .then()
                .doOnError(Throwable::printStackTrace)
                .doOnSuccess(s -> System.out.println("Sent"))
                .subscribe();

        latch.await(7, TimeUnit.SECONDS);
        assertEquals(0L, latch.getCount());
    }

    @Test
    public void testConsumeAgain() throws InterruptedException {
        CountDownLatch latch = new CountDownLatch(2);
        AtomicBoolean shouldStop = new AtomicBoolean(true);
        StringBuffer result = new StringBuffer();

        KafkaSender sender = KafkaSender.create(senderOptions()
                .producerProperty(ProducerConfig.TRANSACTIONAL_ID_CONFIG, "SampleTxn"));

        KafkaReceiver<Integer, String> receiver = KafkaReceiver.create(receiverOptions("consume.again.In"));
        receiver
                .receiveExactlyOnce(sender.transactionManager())
                .doOnNext(m -> System.out.println("Received batch:" + m))
                .concatMap(f -> f
                        .doOnNext(i -> System.out.println("Received item:" + i.value()))
                        .doOnNext(i -> {
                            if (shouldStop.getAndSet(false)) {
                                throw new RuntimeException("Aborted when item 1 is first seen");
                            }
                        })
                        .doOnNext(i -> result.append(i.value()))
                        .doOnNext(i -> latch.countDown())
                        .onErrorResume(e ->
                                receiver.doOnConsumer(consumer -> consumer.assignment().stream()
                                        .peek(tp -> {
                                            if(consumer.committed(tp) != null) {
                                                consumer.seek(tp, consumer.committed(tp).offset());
                                            } else {
                                                consumer.seekToBeginning(Collections.singleton(tp));
                                            }
                                        }).count())
                                        .then(sender.transactionManager().abort())))
                .subscribe();

        Thread.sleep(500);

        KafkaSender.create(senderOptions())
                .send(Flux.range(0, 2)
                        .map(x -> SenderRecord.create("consume.again.In", null, null, 1, "hello-" + x, 1)))
                .then()
                .doOnError(Throwable::printStackTrace)
                .doOnSuccess(s -> System.out.println("Sent"))
                .subscribe();

        latch.await(60, TimeUnit.SECONDS);
        assertEquals(0L, latch.getCount());
        assertEquals("hello-0hello-1hello-2", result.toString());
    }
}
