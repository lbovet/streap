package io.streap.kafka;

import io.streap.core.idempotence.OffsetStore;
import io.streap.kafka.block.ExactlyOnceBlock;
import io.streap.spring.JdbcOffsetStore;
import io.streap.spring.PlatformTransaction;
import io.streap.test.EmbeddedDatabaseSupport;
import io.streap.test.EmbeddedKafkaSupport;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;
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
import static org.apache.kafka.clients.producer.ProducerConfig.TRANSACTIONAL_ID_CONFIG;
import static org.junit.Assert.assertEquals;

@Slf4j
public class ExactlyOnceBlockTest extends EmbeddedDatabaseSupport {

    @BeforeClass
    public static void init() {
        EmbeddedKafkaSupport.init();
    }

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
            log.info("Wrote " + name);
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
                .doOnNext(b -> log.info("Got batch"))
                .compose(ExactlyOnceBlock.<Integer, String>createBlock(transactionManager,
                        PlatformTransaction.supplier(transactionTemplate)).transformer())
                .concatMap(block -> block.items()
                        .map(ConsumerRecord::value)
                        .flatMap(block.wrap(saveName))
                        .map(name -> SenderRecord.create(confirmationTopic, null, null, 1, name, 1))
                        .compose(confirmationSender::send)
                        .then(block.commit())
                        .onErrorResume(e -> block.abort()))
                .subscribe();

        // Receive Confirmations
        KafkaReceiver
                .create(receiverOptions(confirmationTopic))
                .receive()
                .doOnNext(m -> log.info("Received:" + m.value()))
                .doOnNext(m -> latch.countDown())
                .subscribe();

        // Send the names
        KafkaSender.create(senderOptions())
                .send(Flux.fromArray(names)
                        .map(name -> SenderRecord.create(nameTopic, null, null, 1, name, 1)))
                .then()
                .doOnError(Throwable::printStackTrace)
                .doOnSuccess(s -> log.info("Sent"))
                .subscribe();

        latch.await(10, TimeUnit.SECONDS);
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
            log.info("Wrote " + name);
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
                .create(receiverOptions(nameTopic))
                .receiveExactlyOnce(transactionManager)
                .doOnNext(b -> log.info("Got batch" + b))
                .compose(ExactlyOnceBlock.<Integer, String>createBlock(transactionManager,
                        PlatformTransaction.supplier(transactionTemplate)).transformer())
                .concatMap(block -> block.items()
                        .map(ConsumerRecord::value)
                        .flatMap(block.wrap(saveName))
                        .map(name -> SenderRecord.create(confirmationTopic, null, null, 1, name, 1))
                        .compose(confirmationSender::send)
                        .then(block.commit())
                        .onErrorResume(e -> {
                            log.info(e.getMessage());
                            return block.abort();
                        }))
                .subscribe();

        List<String> results = new ArrayList<>();

        // Receive Confirmations
        KafkaReceiver
                .create(receiverOptions(confirmationTopic))
                .receive()
                .doOnNext(m -> log.info("Received:" + m.value()))
                .doOnNext(m -> results.add(m.value()))
                .subscribe();

        // Send the names
        KafkaSender.create(senderOptions())
                .send(Flux.fromArray(names)
                        .map(name -> SenderRecord.create(nameTopic, null, null, 1, name, 1)))
                .then()
                .doOnSuccess(s -> log.info("Sent"))
                .subscribe();

        Thread.sleep(2000);

        assertEquals(0, results.size());

        assertEquals(0, jdbcTemplate
                .queryForList("SELECT NAME FROM PERSON")
                .stream()
                .map(l -> l.get("NAME"))
                .count());
    }

    @Ignore
    @Test
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
                        PlatformTransaction.supplier(transactionTemplate)).with(offsetStore).transformer())
                .doOnNext(System.out::println)
                .concatMap(block -> block.items()
                        .flatMap(block.wrapOnce(x -> results.add("once: " + x.value())))
                        .flatMap(block.wrap(x -> {
                            results.add("twice: " + x.value());
                            return x;
                        }))
                        .last()
                        .flatMap(x -> {
                            if (results.size() < 15) {
                                log.info("ABORT");
                                return block.abort();
                            } else {
                                log.info("COMMIT");
                                return block.commit();
                            }
                        })
                        .onErrorResume(e -> {
                            e.printStackTrace(System.err);
                            return Mono.just("ok");
                        })
                ).subscribe();

        // Send the names
        KafkaSender.create(senderOptions())
                .send(Flux.fromArray(names)
                        .map(name -> SenderRecord.create(nameTopic, null, null, 1, name, 1)))
                .then()
                .doOnSuccess(s -> log.info("Sent"))
                .subscribe();

        Thread.sleep(5000);

        assertEquals(15, results.size());
    }

}
