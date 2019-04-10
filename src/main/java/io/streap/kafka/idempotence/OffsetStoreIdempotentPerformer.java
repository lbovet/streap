package io.streap.kafka.idempotence;

import io.streap.core.idempotence.IdempotentPerformer;
import io.streap.core.idempotence.OffsetStore;
import org.apache.kafka.clients.consumer.ConsumerRecord;
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
