package io.streap.core;

import reactor.core.publisher.Mono;

import java.util.function.Consumer;
import java.util.function.Function;

/**
 * Block providing idempotence by skipping non-idempotent operations.
 */
public interface IdempotentBlock<T> extends ProcessingBlock<T>, IdempotentContext<T> {

}
