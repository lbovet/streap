package io.streap.test;

import kafka.server.KafkaConfig;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import reactor.kafka.receiver.ReceiverOptions;
import reactor.kafka.sender.SenderOptions;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class EmbeddedKafkaSupport {

    private static EmbeddedKafkaBroker kafka;
    private static int count = 0;

    public static void init() {
        if(kafka == null) {
            kafka = new EmbeddedKafkaBroker(1, true, 0)
                    .brokerProperty(KafkaConfig.TransactionsTopicReplicationFactorProp(), "1")
                    .brokerProperty(KafkaConfig.TransactionsTopicMinISRProp(), "1");
            kafka.afterPropertiesSet();
        }
        try {
            Thread.sleep(800);
        } catch (InterruptedException e) {
        }
    }

    public static SenderOptions<Integer,String> senderOptions() {
        Map<String, Object> props = new HashMap<>();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBrokersAsString());
        props.put(ProducerConfig.CLIENT_ID_CONFIG, "sample-producer-"+(count++));
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, IntegerSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        return SenderOptions.<Integer,String>create(props).maxInFlight(1024);
    }

    public static SenderOptions<Integer,String> senderOptions(String txId) {
        return senderOptions().producerProperty(ProducerConfig.TRANSACTIONAL_ID_CONFIG, txId);
    }

    public static ReceiverOptions<Integer, String> receiverOptions(String topic) {
        Map<String, Object> consumerProps = new HashMap<>();
        consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBrokersAsString());
        consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, "sample-group-"+(count++));
        consumerProps.put(ConsumerConfig.CLIENT_ID_CONFIG, "sample-consumer-"+(count++));
        consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, IntegerDeserializer.class);
        consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        consumerProps.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed");
        return ReceiverOptions.<Integer,String>create(consumerProps)
                .subscription(Collections.singleton(topic));
    }
}
