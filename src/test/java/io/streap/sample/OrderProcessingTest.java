package io.streap.sample;

import io.streap.test.EmbeddedKafkaSupport;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;
import reactor.core.publisher.Mono;
import reactor.kafka.receiver.KafkaReceiver;
import reactor.kafka.receiver.ReceiverOptions;
import reactor.kafka.sender.KafkaSender;
import reactor.kafka.sender.SenderOptions;
import reactor.kafka.sender.SenderRecord;

import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CountDownLatch;

@RunWith(SpringRunner.class)
@SpringBootTest
@SpringBootApplication
@EnableAutoConfiguration
public class OrderProcessingTest extends EmbeddedKafkaSupport {

    @Test
    public void testProcessing() throws InterruptedException {
        process();
    }

    public void process() throws InterruptedException {

        String bootstrapServers = System.getProperty("spring.kafka.bootstrap-servers");
        String topic = "orders";

        Map<String, Object> props = new HashMap<>();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, IntegerSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);

        SenderOptions<Integer, String> senderOptions =
                SenderOptions.<Integer, String>create(props).maxInFlight(1024);


        Map<String, Object> consumerProps = new HashMap<>();
        consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, "sample-group");
        consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, IntegerDeserializer.class);
        consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        consumerProps.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");

        CountDownLatch latch = new CountDownLatch(1);

        KafkaSender<Integer,String> sender = KafkaSender.create(senderOptions);
                sender
                .send(Mono.just(SenderRecord.create(topic, null, null, 1, "hello", 1)))
                .then()
                .doOnError(e -> e.printStackTrace(System.err))
                .doOnSuccess(s -> System.err.println("Sends succeeded"))
                .subscribe();

        ReceiverOptions<Integer, String> receiverOptions =
                ReceiverOptions.<Integer, String>create(consumerProps)
                        .subscription(Collections.singleton(topic));
        KafkaReceiver
                .create(receiverOptions)
                .receive()
                .doOnNext(m -> {
                    System.err.println("HEY:" + m.value());
                    latch.countDown();
                })
                .doOnError(e -> e.printStackTrace(System.err))
                .subscribe();

        latch.await();
    }
}
