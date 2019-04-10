package io.streap.core.idempotence;

import io.streap.core.context.Context;
import reactor.core.publisher.Mono;

import java.util.function.Consumer;
import java.util.function.Function;

/**
 * Context providing idempotence by skipping non-idempotent operations.
 */
public interface IdempotentContext<T> extends Context, IdempotentPerformer<T> {

    /**
     * Wraps an non-idempotent operation producing a side effect inside this context.
     * This operation will not be run again. E.g. when events are replayed after failure due to broker unavailability.
     * <p>
     * Use {@link Context#wrap(Function)} for running idempotent operations.
     * They will be run again. E.g. when events are replayed after failure due to broker unavailability.
     */
    default Function<T, Mono<T>> wrapOnce(Consumer<T> operation) {
        return doOnce((t) -> {
            wrap((T u) -> {
                operation.accept(u);
                return u;
            }).apply(t);
            return Mono.empty();
        });
    }
}
