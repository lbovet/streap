package io.streap.kafka;

import io.streap.spring.PlatformTransactionBlock;
import io.streap.test.EmbeddedDatabaseSupport;
import io.streap.test.EmbeddedKafkaSupport;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.junit.ClassRule;
import org.junit.Test;
import org.reactivestreams.Publisher;
import org.springframework.kafka.test.rule.KafkaEmbedded;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.kafka.receiver.KafkaReceiver;
import reactor.kafka.sender.KafkaSender;
import reactor.kafka.sender.SenderRecord;
import reactor.kafka.sender.TransactionManager;

import java.util.Arrays;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;

import static io.streap.test.EmbeddedKafkaSupport.receiverOptions;
import static io.streap.test.EmbeddedKafkaSupport.senderOptions;
import static org.apache.kafka.clients.producer.ProducerConfig.TRANSACTIONAL_ID_CONFIG;
import static org.junit.Assert.assertEquals;

public class ExactlyOnceBlockTest extends EmbeddedDatabaseSupport {

    @ClassRule
    public static KafkaEmbedded embeddedKafka = EmbeddedKafkaSupport.init();


    @Test
    public void testCommit() throws InterruptedException {
        jdbcTemplate.execute("CREATE TABLE PERSON (NAME VARCHAR)");

        Function<String,String> saveName = (name) -> {
            jdbcTemplate.update("INSERT INTO PERSON(NAME) VALUES (?)", name);
            System.out.println("Wrote "+name);
            return name;
        };

        String nameTopic = "test.commit.Name";
        String confirmationTopic = "test.commit.Confirmation";

        KafkaSender<Integer, String> confirmationSender = KafkaSender.create(senderOptions()
                .producerProperty(TRANSACTIONAL_ID_CONFIG, "txid"));
        TransactionManager transactionManager = confirmationSender.transactionManager();

        String[] names = { "john", "paul", "mary", "peter", "luke" };
        CountDownLatch latch = new CountDownLatch(names.length);

        // Main pipeline. Receive names, save them and send confirmations.
        KafkaReceiver
                .create(receiverOptions(nameTopic))
                .receiveExactlyOnce(transactionManager)
                .doOnNext(b-> System.out.println("Got batch"+ b))
                .concatMap(b -> b)
                .doOnNext(b-> System.out.println("Got item"+ b))
                /*
                .compose(ExactlyOnceBlock.<Integer, String>createBlock(transactionManager,
                        PlatformTransactionBlock.supplier(transactionTemplate)).transformer())
                .concatMap(block -> block.items()
                        .map(ConsumerRecord::value)
                        .doOnNext(name -> System.out.println("Processing "+name))
                        //.flatMap(block.wrap(saveName))
                        .map(name -> SenderRecord.create(confirmationTopic, null, null, 1, name, 1))
                        .compose(confirmationSender::send)
                        .doOnComplete(block::commit)
                        .doOnError(e -> block.abort()))*/
                .subscribe();

        // Receive Confirmations
        KafkaReceiver
                .create(receiverOptions(confirmationTopic))
                .receive()
                .doOnNext(m -> System.out.println("Received:" + m.value()))
                .doOnNext(m -> latch.countDown())
                .doOnError(Throwable::printStackTrace)
                .subscribe();

        // Send the names
        KafkaSender.create(senderOptions())
                .send(Flux.fromArray(names)
                        .map(name -> SenderRecord.create(nameTopic, null, null, 1, name, 1)))
                .then()
                .doOnError(Throwable::printStackTrace)
                .doOnSuccess(s -> System.out.println("Sent"))
                .subscribe();

        latch.await(5, TimeUnit.SECONDS);
        assertEquals(0L, latch.getCount());

        System.out.println(jdbcTemplate
                .queryForList("SELECT NAME FROM PERSON"));

        assertEquals(Arrays.asList(names), jdbcTemplate
                .queryForList("SELECT NAME FROM PERSON")
                .stream()
                .map(l -> l.get("name"))
                .collect(Collectors.toList()));
    }
}
