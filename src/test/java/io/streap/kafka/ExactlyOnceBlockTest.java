package io.streap.kafka;

import io.streap.core.OffsetStore;
import io.streap.spring.JdbcOffsetStore;
import io.streap.spring.PlatformTransactionBlock;
import io.streap.test.EmbeddedDatabaseSupport;
import io.streap.test.EmbeddedKafkaSupport;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Ignore;
import org.junit.Test;
import org.springframework.kafka.test.rule.KafkaEmbedded;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.kafka.receiver.KafkaReceiver;
import reactor.kafka.sender.KafkaSender;
import reactor.kafka.sender.SenderRecord;
import reactor.kafka.sender.TransactionManager;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;

import static io.streap.test.EmbeddedKafkaSupport.receiverOptions;
import static io.streap.test.EmbeddedKafkaSupport.senderOptions;
import static io.streap.test.EmbeddedKafkaSupport.waitForAssignment;
import static org.apache.kafka.clients.producer.ProducerConfig.TRANSACTIONAL_ID_CONFIG;
import static org.junit.Assert.assertEquals;

public class ExactlyOnceBlockTest extends EmbeddedDatabaseSupport {

    @ClassRule
    public static KafkaEmbedded embeddedKafka = EmbeddedKafkaSupport.init();

    @Before
    public void setUp() {
        super.setUp();
        jdbcTemplate.execute("CREATE TABLE PERSON (NAME VARCHAR)");
    }

    private String[] names = {"john", "paul", "mary", "peter", "luke"};

    @Test
    public void testCommit() throws InterruptedException {

        String nameTopic = "test.commit.Name";
        String confirmationTopic = "test.commit.Confirmation";

        Function<String, String> saveName = (name) -> {
            jdbcTemplate.update("INSERT INTO PERSON(NAME) VALUES (?)", name);
            System.out.println("Wrote " + name);
            return name;
        };

        KafkaSender<Integer, String> confirmationSender = KafkaSender.create(senderOptions()
                .producerProperty(TRANSACTIONAL_ID_CONFIG, "txn-commit"));
        TransactionManager transactionManager = confirmationSender.transactionManager();

        CountDownLatch latch = new CountDownLatch(names.length);

        // Main pipeline. Receive names, save them and send confirmations.
        KafkaReceiver
                .create(receiverOptions(nameTopic))
                .receiveExactlyOnce(transactionManager)
                .doOnNext(b -> System.out.println("Got batch"))
                .compose(ExactlyOnceBlock.<Integer, String>createBlock(transactionManager,
                        PlatformTransactionBlock.supplier(transactionTemplate)).transformer())
                .concatMap(block -> block.items()
                        .map(ConsumerRecord::value)
                        .flatMap(block.wrap(saveName))
                        .map(name -> SenderRecord.create(confirmationTopic, null, null, 1, name, 1))
                        .compose(confirmationSender::send)
                        .then(block.commit())
                        .onErrorResume(e -> block.abort()))
                .subscribe();

        waitForAssignment();

        // Receive Confirmations
        KafkaReceiver
                .create(receiverOptions(confirmationTopic))
                .receive()
                .doOnNext(m -> System.out.println("Received:" + m.value()))
                .doOnNext(m -> latch.countDown())
                .doOnError(Throwable::printStackTrace)
                .subscribe();

        waitForAssignment();

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

        assertEquals(Arrays.asList(names), jdbcTemplate
                .queryForList("SELECT NAME FROM PERSON")
                .stream()
                .map(l -> l.get("NAME"))
                .collect(Collectors.toList()));
    }

    @Test
    public void testAbort() throws InterruptedException {

        String nameTopic = "test.abort.Name";
        String confirmationTopic = "test.abort.Confirmation";

        Function<String, String> saveName = (name) -> {
            jdbcTemplate.update("INSERT INTO PERSON(NAME) VALUES (?)", name);
            System.out.println("Wrote " + name);
            if (name.equals("paul")) {
                throw new RuntimeException("I don't like him");
            }
            return name;
        };

        KafkaSender<Integer, String> confirmationSender = KafkaSender.create(senderOptions()
                .producerProperty(TRANSACTIONAL_ID_CONFIG, "txn-abort"));
        TransactionManager transactionManager = confirmationSender.transactionManager();

        // Main pipeline. Receive names, save them and send confirmations.
        KafkaReceiver
                .create(receiverOptions(nameTopic)
                        .consumerProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false")
                        .consumerProperty(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 5))
                .receiveExactlyOnce(transactionManager)
                .doOnNext(b -> System.out.println("Got batch" + b))
                .compose(ExactlyOnceBlock.<Integer, String>createBlock(transactionManager,
                        PlatformTransactionBlock.supplier(transactionTemplate)).transformer())
                .concatMap(block -> block.items()
                        .map(ConsumerRecord::value)
                        .flatMap(block.wrap(saveName))
                        .map(name -> SenderRecord.create(confirmationTopic, null, null, 1, name, 1))
                        .compose(confirmationSender::send)
                        .then(block.commit())
                        .onErrorResume(e -> {
                            System.out.println(e.getMessage());
                            return block.abort();
                        }))
                .subscribe();

        waitForAssignment();

        List<String> results = new ArrayList<>();

        // Receive Confirmations
        KafkaReceiver
                .create(receiverOptions(confirmationTopic))
                .receive()
                .doOnNext(m -> System.out.println("Received:" + m.value()))
                .doOnNext(m -> results.add(m.value()))
                .doOnError(Throwable::printStackTrace)
                .subscribe();

        waitForAssignment();

        // Send the names
        KafkaSender.create(senderOptions())
                .send(Flux.fromArray(names)
                        .map(name -> SenderRecord.create(nameTopic, null, null, 1, name, 1)))
                .then()
                .doOnError(Throwable::printStackTrace)
                .doOnSuccess(s -> System.out.println("Sent"))
                .subscribe();

        Thread.sleep(2000);

        assertEquals(0, results.size());

        assertEquals(0, jdbcTemplate
                .queryForList("SELECT NAME FROM PERSON")
                .stream()
                .map(l -> l.get("NAME"))
                .count());
    }

    @Test
    @Ignore
    public void testOnce() throws InterruptedException {
        JdbcOffsetStore.createTable(jdbcTemplate, "offsets");
        OffsetStore offsetStore = new JdbcOffsetStore(jdbcTemplate, "offsets", "test.once");
        String nameTopic = "test.once.Name";

        KafkaSender<Integer, String> confirmationSender = KafkaSender.create(senderOptions()
                .producerProperty(TRANSACTIONAL_ID_CONFIG, "txn-once"));
        TransactionManager transactionManager = confirmationSender.transactionManager();

        List<String> results = new ArrayList<>();

        // Main pipeline. Receive names, save them.
        KafkaReceiver
                .create(receiverOptions(nameTopic))
                .receiveExactlyOnce(transactionManager)
                .compose(ExactlyOnceBlock.<Integer, String>createBlock(transactionManager,
                        PlatformTransactionBlock.supplier(transactionTemplate)).with(offsetStore).transformer())
                .doOnNext(x -> System.out.println(x))
                .concatMap(block -> block.items()
                        .flatMap(block.wrapOnce(x -> results.add("once: " + x.value())))
                        .flatMap(block.wrap(x -> {
                            results.add("twice: " + x.value());
                            return x;
                        }))
                        .last() /*
                        .flatMap( x -> {
                            if(results.size() < 15) {
                                System.out.println("ABORT");
                                return block.abort();
                            } else {
                                System.out.println("COMMIT");
                                return block.commit();
                            }
                        })*/
                        .then(block.abort())
                        .onErrorResume(e -> {
                            e.printStackTrace(System.err);
                            return Mono.just("ok");
                        })
                ).subscribe();

        waitForAssignment();

        // Send the names
        KafkaSender.create(senderOptions())
                .send(Flux.fromArray(names)
                        .map(name -> SenderRecord.create(nameTopic, null, null, 1, name, 1)))
                .then()
                .doOnError(Throwable::printStackTrace)
                .doOnSuccess(s -> System.out.println("Sent"))
                .subscribe();

                        Thread.sleep(2000);

        assertEquals(15, results.size());
    }

}
