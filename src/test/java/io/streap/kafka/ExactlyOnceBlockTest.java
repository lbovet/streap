package io.streap.kafka;

import io.streap.core.OffsetStore;
import io.streap.spring.JdbcOffsetStore;
import io.streap.spring.PlatformTransactionBlock;
import io.streap.test.EmbeddedDatabaseSupport;
import io.streap.test.EmbeddedKafkaSupport;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.springframework.kafka.test.rule.KafkaEmbedded;
import reactor.core.publisher.Flux;
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

public class ExactlyOnceBlockTest extends EmbeddedDatabaseSupport {

    @ClassRule
    public static KafkaEmbedded embeddedKafka = EmbeddedKafkaSupport.init();

    @Before
    public void setUp() {
        super.setUp();
        jdbcTemplate.execute("CREATE TABLE PERSON (NAME VARCHAR)");
    }

    private String[] names = { "john", "paul", "mary", "peter", "luke" };

    @Test
    public void testCommit() throws InterruptedException {

        String nameTopic = "test.commit.Name";
        String confirmationTopic = "test.commit.Confirmation";

        Function<String,String> saveName = (name) -> {
            jdbcTemplate.update("INSERT INTO PERSON(NAME) VALUES (?)", name);
            System.out.println("Wrote "+name);
            return name;
        };

        KafkaSender<Integer, String> confirmationSender = KafkaSender.create(senderOptions()
                .producerProperty(TRANSACTIONAL_ID_CONFIG, "txn"));
        TransactionManager transactionManager = confirmationSender.transactionManager();

        CountDownLatch latch = new CountDownLatch(names.length);

        // Main pipeline. Receive names, save them and send confirmations.
        KafkaReceiver
                .create(receiverOptions(nameTopic))
                .receiveExactlyOnce(transactionManager)
                .doOnNext(b-> System.out.println("Got batch"))
                .compose(ExactlyOnceBlock.<Integer, String>createBlock(transactionManager,
                        PlatformTransactionBlock.supplier(transactionTemplate)).transformer())
                .concatMap(block -> block.items()
                        .map(ConsumerRecord::value)
                        .flatMap(block.wrap(saveName))
                        .map(name -> SenderRecord.create(confirmationTopic, null, null, 1, name, 1))
                        .compose(confirmationSender::send)
                        .doOnComplete(block::commit)
                        .doOnError(e -> block.abort()))
                .subscribe();

        // Receive Confirmations
        KafkaReceiver
                .create(receiverOptions(confirmationTopic))
                .receive()
                .doOnNext(m -> System.out.println("Received:" + m.value()))
                .doOnNext(m -> latch.countDown())
                .doOnError(Throwable::printStackTrace)
                .subscribe();

        Thread.sleep(800);

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

        Function<String,String> saveName = (name) -> {
            jdbcTemplate.update("INSERT INTO PERSON(NAME) VALUES (?)", name);
            System.out.println("Wrote "+name);
            if(name.equals("paul")) {
                throw new RuntimeException("I don't like him");
            }
            return name;
        };

        KafkaSender<Integer, String> confirmationSender = KafkaSender.create(senderOptions()
                .producerProperty(TRANSACTIONAL_ID_CONFIG, "txn"));
        TransactionManager transactionManager = confirmationSender.transactionManager();

        // Main pipeline. Receive names, save them and send confirmations.
        KafkaReceiver
                .create(receiverOptions(nameTopic))
                .receiveExactlyOnce(transactionManager)
                .doOnNext(b -> System.out.println("Got batch"+ b))
                .compose(ExactlyOnceBlock.<Integer, String>createBlock(transactionManager,
                        PlatformTransactionBlock.supplier(transactionTemplate)).transformer())
                .concatMap(block -> block.items()
                        .map(ConsumerRecord::value)
                        .flatMap(block.wrap(saveName))
                        .map(name -> SenderRecord.create(confirmationTopic, null, null, 1, name, 1))
                        .compose(confirmationSender::send)
                        .doOnComplete(block::commit)
                        .doOnError(e -> {
                            System.out.println(e.getMessage());
                            block.abort();
                        }))
                .subscribe();

        List<String> results = new ArrayList<>();

        // Receive Confirmations
        KafkaReceiver
                .create(receiverOptions(confirmationTopic))
                .receive()
                .doOnNext(m -> System.out.println("Received:" + m.value()))
                .doOnNext(m -> results.add(m.value()))
                .doOnError(Throwable::printStackTrace)
                .subscribe();

        Thread.sleep(800);

        // Send the names
        KafkaSender.create(senderOptions())
                .send(Flux.fromArray(names)
                        .map(name -> SenderRecord.create(nameTopic, null, null, 1, name, 1)))
                .then()
                .doOnError(Throwable::printStackTrace)
                .doOnSuccess(s -> System.out.println("Sent"))
                .subscribe();

        Thread.sleep(5000);

        assertEquals(0, results.size());

        assertEquals(0, jdbcTemplate
                .queryForList("SELECT NAME FROM PERSON")
                .stream()
                .map(l -> l.get("NAME"))
                .count());
    }

    @Test
    public void testOnce() throws InterruptedException {
        JdbcOffsetStore.createTable(jdbcTemplate, "offsets");
        OffsetStore offsetStore = new JdbcOffsetStore(jdbcTemplate, "offsets", "test.once");
        String nameTopic = "test.once.Name";

        KafkaSender<Integer, String> confirmationSender = KafkaSender.create(senderOptions()
                .producerProperty(TRANSACTIONAL_ID_CONFIG, "txn"));
        TransactionManager transactionManager = confirmationSender.transactionManager();

        List<String> results = new ArrayList<>();

        // Main pipeline. Receive names, save them and send confirmations.
        Flux<Boolean> receiver = KafkaReceiver
                .create(receiverOptions(nameTopic))
                .receiveExactlyOnce(transactionManager)
                .compose(ExactlyOnceBlock.<Integer, String>createBlock(transactionManager,
                        PlatformTransactionBlock.supplier(transactionTemplate)).with(offsetStore).transformer())
                .concatMap(block -> block.items()
                        .flatMap(block.wrapOnce( x -> results.add("once: "+x.value())))
                        .flatMap(block.wrap( x -> results.add("twice: "+x.value())))
                        .doOnComplete(block::commit)
                        .doOnError(e -> {
                            System.out.println(e.getMessage());
                            block.abort();
                        }));         

        Thread.sleep(800);

        // Send the names
        KafkaSender.create(senderOptions())
                .send(Flux.fromArray(names)
                        .map(name -> SenderRecord.create(nameTopic, null, null, 1, name, 1)))
                .then()
                .doOnError(Throwable::printStackTrace)
                .doOnSuccess(s -> System.out.println("Sent"))
                .subscribe();

        Thread.sleep(5000);

        assertEquals(0, results.size());

    }

}
