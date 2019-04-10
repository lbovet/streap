package io.streap.kafka.processor;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import reactor.core.publisher.Flux;
import reactor.kafka.receiver.ReceiverOptions;

import java.time.Duration;
import java.util.function.Function;

public class ProcessorConfig {

    public static String RETRY_COUNT_CONFIG = "streap.retry.count.config";

    public static <T, K, V> Function<Flux<T>,Flux<T>> configureRetries(ReceiverOptions<K, V> receiverOptions) {
        long retries = (long) receiverOptions.consumerProperties().getOrDefault(ProcessorConfig.RETRY_COUNT_CONFIG, Long.MAX_VALUE);
        long minBackOff = (long) receiverOptions.consumerProperties().getOrDefault(ConsumerConfig.RECONNECT_BACKOFF_MS_CONFIG, 100L);
        long maxBackOff = (long) receiverOptions.consumerProperties().getOrDefault(ConsumerConfig.RECONNECT_BACKOFF_MAX_MS_CONFIG, 1000L);
        return f -> f.retryBackoff(retries, Duration.ofMillis(minBackOff), Duration.ofMillis(maxBackOff));
    }
}
