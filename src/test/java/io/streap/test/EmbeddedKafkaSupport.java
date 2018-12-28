package io.streap.test;

import kafka.server.KafkaConfig;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.kafka.test.rule.KafkaEmbedded;
import reactor.kafka.receiver.ReceiverOptions;
import reactor.kafka.sender.SenderOptions;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class EmbeddedKafkaSupport {

    private static KafkaEmbedded kafka;
    private static int count = 0;

    public static KafkaEmbedded init() {
        if(kafka == null) {
            kafka = new KafkaEmbedded(1, true, 0)
                    .brokerProperty(KafkaConfig.TransactionsTopicReplicationFactorProp(), "1")
                    .brokerProperty(KafkaConfig.TransactionsTopicMinISRProp(), "1");
        }
        return kafka;
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

    public static ReceiverOptions<Integer, String> receiverOptions(String topic) {
        Map<String, Object> consumerProps = new HashMap<>();
        consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBrokersAsString());
        consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, "sample-group-"+(count++));
        consumerProps.put(ConsumerConfig.CLIENT_ID_CONFIG, "sample-consumer-"+(count++));
        consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, IntegerDeserializer.class);
        consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        consumerProps.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed");
        return ReceiverOptions.<Integer,String>create(consumerProps).subscription(Collections.singleton(topic));
    }
}
