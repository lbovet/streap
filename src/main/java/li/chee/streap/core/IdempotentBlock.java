package li.chee.streap.core;

import reactor.core.publisher.Flux;

import java.util.function.Function;

/**
 * Block providing idempotence by skipping non-idempotent operations.
 */
public interface IdempotentBlock<T> extends Block {
    /**
     * Runs an non-idempotent operation producing a side effect.
     * This operation will not be execute again. E.g. when events are replayed after failure due to broker unavailability.
     * <p>
     * Use {@link Block#execute(Function)} for running idempotent operations.
     * They will be execute again. E.g. when events are replayed after failure due to broker unavailability.
     */
    <U> Function<T, Flux<U>> once(Function<T, U> fn);
}
