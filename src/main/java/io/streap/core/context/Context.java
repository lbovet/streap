package io.streap.core.context;

import reactor.core.publisher.Mono;

import java.util.function.Consumer;
import java.util.function.Function;

/**
 * Provides a context for executing operations.
 */
public interface Context {

    /**
     * Wraps a function so that it runs in this context when called.
     */
    <U,V> Function<U,Mono<V>> wrap(Function<U,V> fn);
}
