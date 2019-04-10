package io.streap.kafka.processor;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import reactor.core.publisher.Flux;
import reactor.kafka.receiver.ReceiverOptions;

import java.time.Duration;
import java.util.function.Function;

@Slf4j
public class ErrorHandler {

    public static <T, K, V> Function<Flux<T>, Flux<T>> retry(ReceiverOptions<K, V> receiverOptions) {
        long retries = (long) receiverOptions.consumerProperties().getOrDefault(ProcessorConfig.RETRY_COUNT_CONFIG, Long.MAX_VALUE);
        long minBackOff = (long) receiverOptions.consumerProperties().getOrDefault(ConsumerConfig.RECONNECT_BACKOFF_MS_CONFIG, 100L);
        long maxBackOff = (long) receiverOptions.consumerProperties().getOrDefault(ConsumerConfig.RECONNECT_BACKOFF_MAX_MS_CONFIG, 1000L);
        return f -> f
                .doOnError(e -> log.warn("Error while processing, retrying.", e))
                .retryBackoff(retries, Duration.ofMillis(minBackOff), Duration.ofMillis(maxBackOff));
    }
}
