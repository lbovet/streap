package io.streap.idempotence;

import reactor.core.publisher.Mono;

import java.util.function.Consumer;
import java.util.function.Function;

public interface IdempotentSupport<T> {

    /**
     * Wraps an non-idempotent operation producing a side effect outside of this context.
     * This operation will not be run again. E.g. when events are replayed after failure due to broker unavailability.
     */
    Function<T, Mono<T>> doOnce(Consumer<T> operation);
}
