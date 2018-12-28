package io.streap.core;

import reactor.core.publisher.Mono;

import java.util.function.Consumer;
import java.util.function.Function;

/**
 * Block providing idempotence by skipping non-idempotent operations.
 */
public interface IdempotentBlock<T> extends ProcessingBlock<T> {
    /**
     * Wraps an non-idempotent operation producing a side effect inside the context of the block.
     * This operation will not be run again. E.g. when events are replayed after failure due to broker unavailability.
     * <p>
     * Use {@link Block#wrap(Function)} for running idempotent operations.
     * They will be run again. E.g. when events are replayed after failure due to broker unavailability.
     */
    Function<T, Mono<T>> wrapOnce(Consumer<T> operation);
}
