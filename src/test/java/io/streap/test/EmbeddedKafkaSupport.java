package io.streap.test;

import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.rule.KafkaEmbedded;
import org.springframework.test.annotation.DirtiesContext;

@DirtiesContext
@EmbeddedKafka(partitions = 1, brokerProperties = {"listeners=PLAINTEXT://localhost:9092", "port=9092"})
public abstract class EmbeddedKafkaSupport {


    //FIXME: everything below here is a fix for the IDE - else @EmbeddedKafka should be enough
    @Autowired
    public KafkaEmbedded kafkaEmbedded;

    @ClassRule
    public static KafkaEmbedded embeddedKafka = new KafkaEmbedded(1, true, 0);

    @BeforeClass
    public static void setUpClass() {
        //FIXME: Couldn't find kafka server configurations - kafka server is listening on a random port so i overwrite the client config here should be other way around
        System.setProperty("spring.kafka.bootstrap-servers", embeddedKafka.getBrokersAsString());
        System.setProperty("spring.cloud.stream.kafka.binder.zkNodes", embeddedKafka.getZookeeperConnectionString());
    }

}