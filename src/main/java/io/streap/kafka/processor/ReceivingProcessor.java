/**
 * Copyright (C) 2015 The Gravitee team (http://gravitee.io)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.streap.kafka.processor;

import io.streap.core.context.Context;
import io.streap.core.processor.StreamProcessor;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import reactor.core.publisher.Mono;
import reactor.kafka.receiver.KafkaReceiver;
import reactor.kafka.receiver.ReceiverOptions;

import java.time.Duration;
import java.util.Collections;
import java.util.function.Function;

public abstract class ReceivingProcessor<K, V, C extends Context, S> extends StreamProcessor<ConsumerRecord<K, V>, C, S> {

    protected <T> Function<Throwable, Mono<T>> abortAndResetOffsets(Mono<T> abort, KafkaReceiver<K, V> receiver, ReceiverOptions<K, V> options) {
        long backOff = (long) options.consumerProperties().getOrDefault(ConsumerConfig.RETRY_BACKOFF_MS_CONFIG, 100L);
        return e -> receiver.doOnConsumer(consumer -> {
            consumer.assignment().forEach((tp) -> {
                if (consumer.committed(tp) != null) {
                    consumer.seek(tp, consumer.committed(tp).offset());
                } else {
                    consumer.seekToBeginning(Collections.singleton(tp));
                }
            });
            return null;
        })
                .then(abort)
                .then(Mono.delay(Duration.ofMillis(backOff)))
                .then(Mono.<T>empty());
    }
}
