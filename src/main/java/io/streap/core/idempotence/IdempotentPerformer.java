package io.streap.core.idempotence;

import reactor.core.publisher.Mono;

import java.util.function.Consumer;
import java.util.function.Function;

public interface IdempotentPerformer<T> {

    /**
     * Runs a non-idempotent operation only once.
     * This operation will not be run again. E.g. when events are replayed after failure due to broker unavailability.
     */
    Function<T, Mono<T>> doOnce(Function<T, Mono<Void>> operation);
}
