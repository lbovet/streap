package io.streap.kafka;

import io.streap.test.EmbeddedKafkaSupport;
import org.junit.ClassRule;
import org.junit.Test;
import org.springframework.kafka.test.rule.KafkaEmbedded;
import reactor.core.publisher.Mono;
import reactor.kafka.receiver.KafkaReceiver;
import reactor.kafka.sender.KafkaSender;
import reactor.kafka.sender.SenderRecord;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static io.streap.test.EmbeddedKafkaSupport.receiverOptions;
import static io.streap.test.EmbeddedKafkaSupport.senderOptions;
import static org.junit.Assert.assertEquals;


public class ReactorKafkaTest {

    @ClassRule
    public static KafkaEmbedded embeddedKafka = EmbeddedKafkaSupport.init();

    @Test
    public void testSendReceive() throws InterruptedException {
        CountDownLatch latch = new CountDownLatch(1);

        KafkaSender.create(senderOptions())
                .send(Mono.just(SenderRecord.create("reactor-kafka", null, null, 1, "hello", 1)))
                .then()
                .doOnError(Throwable::printStackTrace)
                .doOnSuccess(s -> System.out.println("Sent"))
                .subscribe();

        KafkaReceiver
                .create(receiverOptions("reactor-kafka"))
                .receive()
                .doOnNext(m -> System.out.println("Received:" + m.value()))
                .doOnNext(m -> latch.countDown())
                .doOnError(Throwable::printStackTrace)
                .subscribe();

        latch.await(5, TimeUnit.SECONDS);
        assertEquals(0L, latch.getCount());
    }
}
