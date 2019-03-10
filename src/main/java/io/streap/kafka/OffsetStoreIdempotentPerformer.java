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
package io.streap.kafka;

import io.streap.idempotence.IdempotentPerformer;
import io.streap.idempotence.OffsetStore;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import reactor.core.publisher.Mono;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;

/**
 * Implements idempotence for kafka records using an offset store.
 */
public class OffsetStoreIdempotentPerformer<U, V> implements IdempotentPerformer<ConsumerRecord<U,V>> {
    private OffsetStore offsetStore;
    private Map<String,Long> lastOffsets = new ConcurrentHashMap<>();

    public OffsetStoreIdempotentPerformer(OffsetStore offsetStore) {
        this.offsetStore = offsetStore;
    }

    @Override
    public Function<ConsumerRecord<U,V>, Mono<ConsumerRecord<U,V>>> doOnce(Function<ConsumerRecord<U,V>, Mono<Void>> operation) {
        return record -> {
            String key = record.topic()+":"+record.partition();
            long lastOffset = lastOffsets.computeIfAbsent( key, (k) -> offsetStore.read(record.partition()));
            if (record.offset() > lastOffset) {
                return operation.apply(record).then(Mono.just(record));
            } else {
                return Mono.just(record);
            }
        };
    }
}
